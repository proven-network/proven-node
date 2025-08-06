//! Partitioned logging system for cluster nodes

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use parking_lot::{Mutex, RwLock};
use rusqlite::{Connection, params, params_from_iter, types::Value};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Write as FmtWrite;
use std::path::Path;
use std::sync::Arc;
use std::{cell::RefCell, time::Duration};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
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

/// Log level for filtering
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogLevel {
    /// Error level
    Error,
    /// Warning level
    Warn,
    /// Info level
    Info,
    /// Debug level
    Debug,
    /// Trace level
    Trace,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Error => write!(f, "ERROR"),
            Self::Warn => write!(f, "WARN"),
            Self::Info => write!(f, "INFO"),
            Self::Debug => write!(f, "DEBUG"),
            Self::Trace => write!(f, "TRACE"),
        }
    }
}

/// A log entry from a node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// The node that generated this log
    pub node_id: String,
    /// Execution order for sorting
    pub execution_order: u32,
    /// Log level
    pub level: LogLevel,
    /// Log message
    pub message: String,
    /// When this log was generated
    pub timestamp: DateTime<Utc>,
    /// Module/component that generated the log
    pub target: Option<String>,
}

/// Commands for the async log writer
#[derive(Debug)]
enum LogWriterCommand {
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

/// Async log writer that runs in background task
struct AsyncLogWriter {
    command_receiver: Receiver<LogWriterCommand>,
    connection: Arc<Mutex<Connection>>,
}

impl AsyncLogWriter {
    const fn new(
        command_receiver: Receiver<LogWriterCommand>,
        connection: Arc<Mutex<Connection>>,
    ) -> Self {
        // Don't use tracing macros during initialization to avoid recursion
        // when this is used as a global tracing subscriber
        Self {
            command_receiver,
            connection,
        }
    }

    #[allow(clippy::cognitive_complexity)]
    async fn run(mut self) {
        // Starting async log writer task

        let mut batch = LogBatch::new();
        let batch_interval = Duration::from_millis(5);
        let max_batch_size = 2000;

        let mut last_flush = std::time::Instant::now();
        let mut interval = tokio::time::interval(batch_interval);

        loop {
            tokio::select! {
                Some(LogWriterCommand::WriteLog(entry)) = self.command_receiver.recv() => {
                    batch.add(entry);

                    if batch.len() >= max_batch_size {
                        if let Err(e) = self.flush_batch(&mut batch) {
                            // Log to file instead of stderr to avoid interfering with TUI
                            tracing::error!("Failed to flush batch: {e}");
                        }
                        last_flush = std::time::Instant::now();
                    }
                }
                _ = interval.tick() => {
                    if !batch.is_empty()
                        && (last_flush.elapsed() >= batch_interval || batch.len() >= 50)
                    {
                        if let Err(e) = self.flush_batch(&mut batch) {
                            // Log to file instead of stderr to avoid interfering with TUI
                            tracing::error!("Failed to flush batch: {e}");
                        }
                        last_flush = std::time::Instant::now();
                    }
                }
                else => {
                    // Channel closed, flush remaining and exit
                    if !batch.is_empty()
                        && let Err(e) = self.flush_batch(&mut batch) {
                            // Log to file instead of stderr to avoid interfering with TUI
                            tracing::error!("Failed to flush final batch: {e}");
                        }
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

        let mut stmt = tx
            .prepare_cached(INSERT_LOG_SQL)
            .context("Failed to prepare insert statement")?;

        for entry in batch.drain() {
            let timestamp = entry.timestamp.timestamp_micros();
            let level = entry.level.to_string();

            stmt.execute(params![
                timestamp,
                level,
                entry.node_id,
                entry.message,
                entry.target,
                i64::from(entry.execution_order)
            ])
            .context("Failed to insert log entry")?;
        }

        drop(stmt);

        tx.commit().context("Failed to commit transaction")?;
        Ok(())
    }
}

/// Extract node ID from the current thread name
/// Thread names follow the pattern "node-{session}-{execution_order}-{node_id_prefix}"
/// (e.g., "node-test-uuid-1-0a66aa74")
/// Returns None for non-node threads (main thread, etc.)
fn extract_node_info_from_thread_name() -> Option<(String, u32)> {
    let thread = std::thread::current();
    let thread_name = thread.name()?;

    // Parse "node-session-1-0a66aa74" -> execution_order = 1
    if let Some(node_part) = thread_name.strip_prefix("node-") {
        let parts: Vec<&str> = node_part.split('-').collect();

        // We expect at least 3 parts: session, execution_order, node_id_prefix
        if parts.len() >= 3 {
            // The execution order is the second-to-last part
            if let Some(exec_order_str) = parts.get(parts.len() - 2)
                && let Ok(execution_order) = exec_order_str.parse::<u32>()
            {
                return Some((thread_name.to_string(), execution_order));
            }
        }
    }

    None
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
    /// Task handle for async log writer
    writer_task: Option<JoinHandle<()>>,
    /// Current log level filter
    current_level_filter: Arc<RwLock<LogLevel>>,
    /// Shared database connection for reading (protected by mutex)
    connection: Arc<Mutex<Connection>>,
    /// Whether to print logs to stdout (for debugging)
    print_to_stdout: bool,
}

impl LogWriter {
    /// Create a new SQL log writer
    ///
    /// # Errors
    ///
    /// Returns an error if creating the database connection fails
    pub fn new(base_dir: &std::path::Path) -> Result<Self> {
        Self::with_stdout(base_dir, false)
    }

    /// Create a new SQL log writer with optional stdout logging
    ///
    /// # Errors
    ///
    /// Returns an error if creating the database connection fails
    pub fn with_stdout(base_dir: &std::path::Path, print_to_stdout: bool) -> Result<Self> {
        // Ensure log directory exists
        std::fs::create_dir_all(base_dir).context("Failed to create log directory")?;

        // Create file-based database connection
        let db_path = base_dir.join("logs.db");
        let connection =
            Connection::open(&db_path).context("Failed to create database connection")?;

        // Create tables
        connection
            .execute(CREATE_TABLE_SQL, params![])
            .context("Failed to create tables")?;

        let connection = Arc::new(Mutex::new(connection));

        // Create async log writer
        let (command_sender, command_receiver) = mpsc::channel(1000);
        let async_writer = AsyncLogWriter::new(command_receiver, Arc::clone(&connection));

        // Start background task
        let writer_task = tokio::spawn(async move {
            async_writer.run().await;
        });

        Ok(Self {
            command_sender,
            writer_task: Some(writer_task),
            current_level_filter: Arc::new(RwLock::new(LogLevel::Info)),
            connection,
            print_to_stdout,
        })
    }

    /// Add a log entry (non-blocking)
    pub fn add_log(&self, entry: LogEntry) {
        // Print to stdout if enabled
        if self.print_to_stdout {
            let level_str = match entry.level {
                LogLevel::Error => "ERROR",
                LogLevel::Warn => "WARN",
                LogLevel::Info => "INFO",
                LogLevel::Debug => "DEBUG",
                LogLevel::Trace => "TRACE",
            };
            eprintln!(
                "[{}] [{}] {} - {}",
                level_str,
                entry.node_id,
                entry.target.as_deref().unwrap_or(""),
                entry.message
            );
        }

        // Try to send without blocking - if channel is full, drop the log
        let _ = self
            .command_sender
            .try_send(LogWriterCommand::WriteLog(entry));
    }

    /// Get the shared database connection
    #[must_use]
    pub fn get_connection(&self) -> Arc<Mutex<Connection>> {
        Arc::clone(&self.connection)
    }
}

impl Drop for LogWriter {
    fn drop(&mut self) {
        // Task will exit when channel is dropped
        // Just abort the task to avoid blocking in drop
        if let Some(handle) = self.writer_task.take() {
            handle.abort();
        }
    }
}

impl Clone for LogWriter {
    fn clone(&self) -> Self {
        // Share the same command sender and connection for cloning
        Self {
            command_sender: self.command_sender.clone(),
            writer_task: None, // Don't clone the task handle - only the original owns it
            current_level_filter: Arc::clone(&self.current_level_filter),
            connection: Arc::clone(&self.connection),
            print_to_stdout: self.print_to_stdout,
        }
    }
}

/// Integrated tracing layer that captures logs and writes them to SQL
impl<S> Layer<S> for LogWriter
where
    S: Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_event(&self, event: &Event<'_>, _ctx: LayerContext<'_, S>) {
        // Extract node ID from thread name, use "main" for non-node threads
        let (node_id, execution_order) =
            extract_node_info_from_thread_name().unwrap_or_else(|| ("main".to_string(), 0));

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
            execution_order,
            level,
            message: visitor.message,
            timestamp: Utc::now(),
            target,
        };

        // Send to SQL-based log writer
        self.add_log(log_entry);
    }
}

/// Cluster-wide logging system
pub struct ClusterLogSystem {
    writer: Arc<LogWriter>,
}

impl ClusterLogSystem {
    /// Create a new cluster log system
    ///
    /// # Errors
    ///
    /// Returns an error if creating the log writer fails
    pub fn new(base_dir: &Path) -> Result<Self> {
        let writer = LogWriter::new(base_dir)?;
        Ok(Self {
            writer: Arc::new(writer),
        })
    }

    /// Create a new cluster log system with stdout logging enabled
    ///
    /// # Errors
    ///
    /// Returns an error if creating the log writer fails
    pub fn with_stdout(base_dir: &Path) -> Result<Self> {
        let writer = LogWriter::with_stdout(base_dir, true)?;
        Ok(Self {
            writer: Arc::new(writer),
        })
    }

    /// Setup global tracing subscriber with this log system
    /// This should be called once at the start of tests or applications
    ///
    /// # Errors
    ///
    /// Returns an error if the tracing subscriber is already set
    pub fn setup_global_subscriber(&self) -> Result<()> {
        use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

        let layer = self.get_writer();

        tracing_subscriber::registry()
            .with(layer)
            .with(tracing_subscriber::filter::LevelFilter::from_level(
                tracing::Level::TRACE,
            ))
            .try_init()
            .map_err(|e| anyhow::anyhow!("Failed to setup tracing subscriber: {}", e))?;

        Ok(())
    }

    /// Get a clone of the log writer for use as a tracing layer
    #[must_use]
    pub fn get_writer(&self) -> LogWriter {
        self.writer.as_ref().clone()
    }

    /// Get the database connection for querying logs
    #[must_use]
    pub fn get_connection(&self) -> Arc<Mutex<Connection>> {
        self.writer.get_connection()
    }
}

/// Log filtering options
#[derive(Debug, Clone, Default)]
pub struct LogFilter {
    /// Filter by log level (minimum level)
    pub level: Option<LogLevel>,
    /// Filter by node ID
    pub node_id: Option<String>,
    /// Filter logs since this time
    pub since: Option<DateTime<Utc>>,
    /// Limit number of results
    pub limit: Option<usize>,
}

impl LogFilter {
    /// Query logs from the database with the given filter
    ///
    /// # Errors
    ///
    /// Returns an error if the SQL query fails
    pub fn query_logs(&self, connection: &Connection) -> Result<Vec<LogEntry>> {
        let mut sql = String::from(
            "SELECT timestamp, level, node_id, message, target, execution_order FROM logs",
        );
        let mut conditions = Vec::new();
        let mut params: Vec<Value> = Vec::new();

        // Add level filter
        if let Some(level) = &self.level {
            // Include all levels at or above the specified level
            match level {
                LogLevel::Error => {
                    conditions.push("level = ?");
                    params.push(Value::Text("ERROR".to_string()));
                }
                LogLevel::Warn => {
                    conditions.push("level IN (?, ?)");
                    params.extend(vec![
                        Value::Text("ERROR".to_string()),
                        Value::Text("WARN".to_string()),
                    ]);
                }
                LogLevel::Info => {
                    conditions.push("level IN (?, ?, ?)");
                    params.extend(vec![
                        Value::Text("ERROR".to_string()),
                        Value::Text("WARN".to_string()),
                        Value::Text("INFO".to_string()),
                    ]);
                }
                LogLevel::Debug => {
                    conditions.push("level IN (?, ?, ?, ?)");
                    params.extend(vec![
                        Value::Text("ERROR".to_string()),
                        Value::Text("WARN".to_string()),
                        Value::Text("INFO".to_string()),
                        Value::Text("DEBUG".to_string()),
                    ]);
                }
                LogLevel::Trace => {
                    conditions.push("level IN (?, ?, ?, ?, ?)");
                    params.extend(vec![
                        Value::Text("ERROR".to_string()),
                        Value::Text("WARN".to_string()),
                        Value::Text("INFO".to_string()),
                        Value::Text("DEBUG".to_string()),
                        Value::Text("TRACE".to_string()),
                    ]);
                }
            }
        }

        // Add node filter
        if let Some(node_id) = &self.node_id {
            conditions.push("node_id = ?");
            params.push(Value::Text(node_id.clone()));
        }

        // Add time filter
        if let Some(since) = &self.since {
            conditions.push("timestamp >= ?");
            params.push(Value::Integer(since.timestamp_micros()));
        }

        // Build WHERE clause
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        // Add ordering
        sql.push_str(" ORDER BY timestamp DESC");

        // Add limit
        if let Some(limit) = self.limit {
            use std::fmt::Write;
            write!(&mut sql, " LIMIT {limit}").unwrap();
        }

        // Execute query
        let mut stmt = connection.prepare(&sql)?;
        let log_iter = stmt.query_map(params_from_iter(params.iter()), |row| {
            let timestamp_micros: i64 = row.get(0)?;
            let level_str: String = row.get(1)?;
            let node_id: String = row.get(2)?;
            let message: String = row.get(3)?;
            let target: Option<String> = row.get(4)?;
            let execution_order: i64 = row.get(5)?;

            let level = match level_str.as_str() {
                "ERROR" => LogLevel::Error,
                "WARN" => LogLevel::Warn,
                "DEBUG" => LogLevel::Debug,
                "TRACE" => LogLevel::Trace,
                _ => LogLevel::Info,
            };

            let timestamp =
                DateTime::from_timestamp_micros(timestamp_micros).unwrap_or_else(Utc::now);

            Ok(LogEntry {
                node_id,
                execution_order: u32::try_from(execution_order).unwrap_or(0),
                level,
                message,
                timestamp,
                target,
            })
        })?;

        let mut logs = Vec::new();
        for log in log_iter {
            logs.push(log?);
        }

        // Reverse to get chronological order (since we queried DESC)
        logs.reverse();

        Ok(logs)
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
            node_id: "test-node".to_string(),
            execution_order: 1,
            level: LogLevel::Info,
            message: "Test log message".to_string(),
            timestamp: Utc::now(),
            target: Some("test".to_string()),
        }
    }

    #[tokio::test]
    async fn test_writer_creation() {
        let base_dir = create_temp_dir();
        let writer = LogWriter::new(&base_dir).expect("Failed to create writer");

        // Test that we can get the connection
        let _conn = writer.get_connection();
    }

    #[tokio::test]
    async fn test_log_writer() {
        let base_dir = create_temp_dir();
        let writer = LogWriter::new(&base_dir).expect("Failed to create writer");

        let entry = create_test_log_entry();
        writer.add_log(entry);

        // Give async writer time to process
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Check that logs were written to database
        let conn = writer.get_connection();
        let conn = conn.lock();

        let mut stmt = conn.prepare("SELECT COUNT(*) FROM logs").unwrap();
        let count: i64 = stmt.query_row([], |row| row.get(0)).unwrap();

        drop(stmt);
        drop(conn);

        assert!(count > 0);
    }

    #[tokio::test]
    async fn test_log_filtering() {
        let base_dir = create_temp_dir();
        let writer = LogWriter::new(&base_dir).expect("Failed to create writer");

        // Add various log entries
        let entries = vec![
            LogEntry {
                node_id: "node-1".to_string(),
                execution_order: 1,
                level: LogLevel::Error,
                message: "Error message".to_string(),
                timestamp: Utc::now(),
                target: Some("test".to_string()),
            },
            LogEntry {
                node_id: "node-1".to_string(),
                execution_order: 1,
                level: LogLevel::Info,
                message: "Info message".to_string(),
                timestamp: Utc::now(),
                target: Some("test".to_string()),
            },
            LogEntry {
                node_id: "node-2".to_string(),
                execution_order: 2,
                level: LogLevel::Debug,
                message: "Debug message".to_string(),
                timestamp: Utc::now(),
                target: Some("test".to_string()),
            },
        ];

        for entry in entries {
            writer.add_log(entry);
        }

        // Give async writer time to process
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Test filtering
        let conn = writer.get_connection();

        // Filter by node
        {
            let filter = LogFilter {
                node_id: Some("node-1".to_string()),
                ..Default::default()
            };
            let logs = filter.query_logs(&conn.lock()).unwrap();
            assert_eq!(logs.len(), 2);
        }

        // Filter by level
        {
            let filter = LogFilter {
                level: Some(LogLevel::Error),
                ..Default::default()
            };
            let logs = filter.query_logs(&conn.lock()).unwrap();
            assert_eq!(logs.len(), 1);
        }
    }
}
