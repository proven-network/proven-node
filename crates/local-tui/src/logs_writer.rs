//! SQL-based in-memory log writing system

use crate::messages::{LogEntry, LogLevel, MAIN_THREAD_NODE_ID, TuiNodeId};
use anyhow::{Context, Result};
use chrono::Utc;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use parking_lot::{Mutex, RwLock};
use rusqlite::{Connection, params};
use std::fmt::Write as FmtWrite;
use std::sync::Arc;
use std::{cell::RefCell, thread, time::Duration};
use tracing::{
    Event, Subscriber, error,
    field::{Field, Visit},
    info,
};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context as LayerContext;

thread_local! {
    /// Thread-local buffer for formatting node IDs to reduce allocations
    static NODE_ID_BUFFER: RefCell<String> = RefCell::new(String::with_capacity(32));
}

/// SQL for creating the logs table
const CREATE_TABLE_SQL: &str = r"
CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    level TEXT NOT NULL CHECK(level IN ('ERROR', 'WARN', 'INFO', 'DEBUG', 'TRACE')),
    node_id TEXT NOT NULL,
    message TEXT NOT NULL,
    target TEXT,
    execution_order INTEGER NOT NULL
);

-- Indexes for efficient filtering
CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level);
CREATE INDEX IF NOT EXISTS idx_logs_node_id ON logs(node_id);
CREATE INDEX IF NOT EXISTS idx_logs_execution_order ON logs(execution_order);
";

/// SQL for inserting a log entry
const INSERT_LOG_SQL: &str = r"
INSERT INTO logs (timestamp, level, node_id, message, target, execution_order)
VALUES (?, ?, ?, ?, ?, ?)
";

/// Commands for the async log writer
#[derive(Debug)]
enum LogWriterCommand {
    Shutdown,
    WriteLog(LogEntry),
}

/// Batch of log entries for efficient insertion
struct LogBatch {
    entries: Vec<LogEntry>,
}

impl LogBatch {
    fn new() -> Self {
        Self {
            entries: Vec::with_capacity(2000),
        }
    }

    fn add(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    const fn len(&self) -> usize {
        self.entries.len()
    }

    fn drain(&mut self) -> Vec<LogEntry> {
        std::mem::take(&mut self.entries)
    }
}

/// Async log writer that runs in background thread
struct AsyncLogWriter {
    command_receiver: Receiver<LogWriterCommand>,
    connection: Arc<Mutex<Connection>>,
}

impl AsyncLogWriter {
    fn new(
        command_receiver: Receiver<LogWriterCommand>,
        connection: Arc<Mutex<Connection>>,
    ) -> Self {
        info!("Created in-memory SQL log writer");
        Self {
            command_receiver,
            connection,
        }
    }

    #[allow(clippy::cognitive_complexity)]
    fn run(self) {
        info!("Starting async log writer thread");

        let mut batch = LogBatch::new();
        // Reduced interval for lower latency while still batching effectively
        let batch_interval = Duration::from_millis(5);
        // Increased batch size to reduce transaction overhead
        let max_batch_size = 2000;

        let mut last_flush = std::time::Instant::now();

        loop {
            // Try to receive with timeout for periodic flushing
            match self.command_receiver.recv_timeout(batch_interval) {
                Ok(LogWriterCommand::WriteLog(entry)) => {
                    batch.add(entry);

                    // Flush if batch is full
                    if batch.len() >= max_batch_size {
                        if let Err(e) = self.flush_batch(&mut batch) {
                            error!("Failed to flush batch: {}", e);
                        }
                        last_flush = std::time::Instant::now();
                    }
                }
                Ok(LogWriterCommand::Shutdown) => {
                    info!("Shutting down async log writer");
                    if !batch.is_empty()
                        && let Err(e) = self.flush_batch(&mut batch)
                    {
                        error!("Failed to flush final batch: {}", e);
                    }
                    break;
                }
                Err(RecvTimeoutError::Timeout) => {
                    // Periodic flush - also flush if we have a reasonable number of logs
                    // This ensures logs appear quickly even under light load
                    if !batch.is_empty()
                        && (last_flush.elapsed() >= batch_interval || batch.len() >= 50)
                    {
                        if let Err(e) = self.flush_batch(&mut batch) {
                            error!("Failed to flush batch: {}", e);
                        }
                        last_flush = std::time::Instant::now();
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    break;
                }
            }
        }
    }

    #[allow(clippy::significant_drop_in_scrutinee)]
    #[allow(clippy::significant_drop_tightening)]
    fn flush_batch(&self, batch: &mut LogBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut conn = self.connection.lock();
        let tx = conn.transaction().context("Failed to start transaction")?;

        // Prepare statement once for the batch
        let mut stmt = tx
            .prepare_cached(INSERT_LOG_SQL)
            .context("Failed to prepare insert statement")?;

        for entry in batch.drain() {
            let timestamp = entry.timestamp.timestamp_micros();
            let level = match entry.level {
                LogLevel::Error => "ERROR",
                LogLevel::Warn => "WARN",
                LogLevel::Info => "INFO",
                LogLevel::Debug => "DEBUG",
                LogLevel::Trace => "TRACE",
            };
            let node_id = if entry.node_id == MAIN_THREAD_NODE_ID {
                "main".to_string()
            } else {
                // Use thread-local buffer to reduce allocations
                NODE_ID_BUFFER.with(|buf| {
                    let mut buffer = buf.borrow_mut();
                    buffer.clear();
                    write!(
                        &mut buffer,
                        "{}-{}",
                        entry.node_id.execution_order(),
                        entry.node_id.pokemon_name()
                    )
                    .unwrap();
                    buffer.clone()
                })
            };
            let execution_order = i64::from(entry.node_id.execution_order());

            stmt.execute(params![
                timestamp,
                level,
                node_id,
                entry.message,
                entry.target,
                execution_order
            ])
            .context("Failed to insert log entry")?;
        }

        // Drop the statement before committing
        drop(stmt);

        tx.commit().context("Failed to commit transaction")?;
        Ok(())
    }
}

/// Extract node ID from the current thread name
/// Thread names follow the pattern "node-{execution_order}-{pokemon_id}" (e.g., "node-1-42", "node-2-7")
/// Returns None for non-node threads (main thread, etc.)
fn extract_node_id_from_thread_name() -> Option<TuiNodeId> {
    let thread = std::thread::current();
    let thread_name = thread.name()?;

    // Parse "node-123-45" -> execution_order = 123, pokemon_id = 45
    let node_part = thread_name.strip_prefix("node-")?;
    let mut parts = node_part.split('-');

    let execution_order: u8 = parts.next()?.parse().ok()?;
    let pokemon_id: u8 = parts.next()?.parse().ok()?;

    Some(TuiNodeId::with_values(execution_order, pokemon_id))
}

/// A visitor for extracting message fields from tracing events
struct MessageVisitor {
    message: String,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        } else if self.message.is_empty() {
            // If no message field, try to capture any field
            if !self.message.is_empty() {
                self.message.push_str(", ");
            }
            write!(self.message, "{}={:?}", field.name(), value).ok();
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else if self.message.is_empty() {
            if !self.message.is_empty() {
                self.message.push_str(", ");
            }
            write!(self.message, "{}={}", field.name(), value).ok();
        }
    }
}

/// High-performance SQL-based log writer with integrated tracing layer
pub struct LogWriter {
    /// Command sender for async log writer
    command_sender: Sender<LogWriterCommand>,
    /// Thread handle for async log writer
    writer_thread: Option<thread::JoinHandle<()>>,
    /// Current log level filter
    current_level_filter: Arc<RwLock<LogLevel>>,
    /// Current node filter
    current_node_filter: Arc<RwLock<Option<TuiNodeId>>>,
    /// Shared database connection for reading (protected by mutex)
    connection: Arc<Mutex<Connection>>,
}

impl LogWriter {
    /// Create a new SQL log writer
    pub fn new(_base_dir: &std::path::Path) -> Result<Self> {
        // Create shared in-memory database connection
        let connection =
            Connection::open_in_memory().context("Failed to create in-memory database")?;

        // Create tables
        connection
            .execute(CREATE_TABLE_SQL, params![])
            .context("Failed to create tables")?;

        let connection = Arc::new(Mutex::new(connection));

        // Create async log writer
        let (command_sender, command_receiver) = crossbeam_channel::unbounded();
        let async_writer = AsyncLogWriter::new(command_receiver, Arc::clone(&connection));

        // Start background thread
        let writer_thread = thread::spawn(move || {
            async_writer.run();
        });

        Ok(Self {
            command_sender,
            writer_thread: Some(writer_thread),
            current_level_filter: Arc::new(RwLock::new(LogLevel::Info)),
            current_node_filter: Arc::new(RwLock::new(None)),
            connection,
        })
    }

    /// Add a log entry (non-blocking)
    pub fn add_log(&self, entry: LogEntry) {
        if let Err(e) = self.command_sender.send(LogWriterCommand::WriteLog(entry)) {
            error!("Failed to send log entry to async writer: {}", e);
        }
    }

    /// Get the shared database connection
    pub fn get_connection(&self) -> Arc<Mutex<Connection>> {
        Arc::clone(&self.connection)
    }
}

impl Drop for LogWriter {
    fn drop(&mut self) {
        // Send shutdown command to async writer
        if let Err(e) = self.command_sender.send(LogWriterCommand::Shutdown) {
            error!("Failed to send shutdown command during drop: {}", e);
        }

        // Wait for writer thread to finish
        if let Some(handle) = self.writer_thread.take()
            && let Err(e) = handle.join()
        {
            error!("Failed to join async writer thread: {:?}", e);
        }
    }
}

impl Clone for LogWriter {
    fn clone(&self) -> Self {
        // Share the same command sender and connection for cloning
        Self {
            command_sender: self.command_sender.clone(),
            writer_thread: None, // Don't clone the thread handle - only the original owns it
            current_level_filter: Arc::clone(&self.current_level_filter),
            current_node_filter: Arc::clone(&self.current_node_filter),
            connection: Arc::clone(&self.connection),
        }
    }
}

/// Integrated tracing layer that captures logs and writes them to SQL
impl<S> Layer<S> for LogWriter
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: LayerContext<'_, S>) {
        // Extract node ID from thread name, use MAIN_THREAD_NODE_ID for non-node threads
        let node_id = extract_node_id_from_thread_name().unwrap_or(MAIN_THREAD_NODE_ID);

        // Extract log level
        let level = match *event.metadata().level() {
            tracing::Level::ERROR => LogLevel::Error,
            tracing::Level::WARN => LogLevel::Warn,
            tracing::Level::INFO => LogLevel::Info,
            tracing::Level::DEBUG => LogLevel::Debug,
            tracing::Level::TRACE => LogLevel::Trace,
        };

        // Extract target
        let target = Some(event.metadata().target().to_string());

        // Ignore targets from certain sources
        if let Some(target_str) = target.as_deref()
            && (target_str.starts_with("openraft")
                || target_str.starts_with("hyper_util")
                || target_str.starts_with("swc_ecma_transforms_base")
                || target_str.starts_with("h2::")
                || target_str.starts_with("hickory")
                || target_str == "log")
        {
            return;
        }

        // Extract message using visitor pattern
        let mut visitor = MessageVisitor {
            message: String::new(),
        };

        event.record(&mut visitor);

        // If no message was extracted, use the event name as fallback
        if visitor.message.is_empty() {
            visitor.message = event.metadata().name().to_string();
        }

        let log_entry = LogEntry {
            node_id,
            level,
            message: visitor.message,
            timestamp: Utc::now(),
            target,
        };

        // Send to SQL-based log writer
        self.add_log(log_entry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_temp_dir() -> std::path::PathBuf {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        temp_dir.path().to_path_buf()
    }

    fn create_test_log_entry() -> LogEntry {
        LogEntry {
            node_id: TuiNodeId::new(),
            level: LogLevel::Info,
            message: "Test log message".to_string(),
            timestamp: Utc::now(),
            target: Some("test".to_string()),
        }
    }

    fn reset_node_id_state() {
        #[cfg(test)]
        crate::node_id::TuiNodeId::reset_global_state();
    }

    #[test]
    fn test_writer_creation() {
        let base_dir = create_temp_dir();
        let writer = LogWriter::new(&base_dir).expect("Failed to create writer");

        // Test that we can get the connection
        let _conn = writer.get_connection();
    }

    #[test]
    fn test_log_writer() {
        reset_node_id_state();
        let base_dir = create_temp_dir();
        let writer = LogWriter::new(&base_dir).expect("Failed to create writer");

        let entry = create_test_log_entry();
        writer.add_log(entry);

        // Give async writer time to process
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Check that logs were written to database
        let conn = writer.get_connection();
        let conn = conn.lock();

        let mut stmt = conn.prepare("SELECT COUNT(*) FROM logs").unwrap();
        let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();

        drop(stmt);
        drop(conn);

        assert!(count > 0);
    }
}
