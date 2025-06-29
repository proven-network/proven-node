//! Disk-based log writing system with session management

use crate::messages::{LogEntry, LogLevel, MAIN_THREAD_NODE_ID, NodeId};
use anyhow::{Context, Result};
use chrono::Utc;
use parking_lot::RwLock;
use std::fmt::Write;
use std::sync::Arc;
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write as IoWrite},
    path::PathBuf,
    sync::mpsc,
    thread,
};
use tracing::{
    Event, Subscriber, debug, error,
    field::{Field, Visit},
    info,
};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context as LayerContext;

/// Commands for the async log writer
#[derive(Debug)]
enum LogWriterCommand {
    WriteLog(LogEntry),
    Flush,
    Shutdown,
}

/// Async log writer that runs in background thread
struct AsyncLogWriter {
    command_receiver: mpsc::Receiver<LogWriterCommand>,
    session_dir: PathBuf,
    config: DiskLogConfig,
    writers: HashMap<LogCategory, BufWriter<File>>,
    last_flush: std::time::Instant,
}

impl AsyncLogWriter {
    fn new(
        command_receiver: mpsc::Receiver<LogWriterCommand>,
        config: DiskLogConfig,
    ) -> Result<Self> {
        let session_dir = config.base_dir.clone();
        fs::create_dir_all(&session_dir).with_context(|| {
            format!(
                "Failed to create session directory: {}",
                session_dir.display()
            )
        })?;

        let mut writer = Self {
            command_receiver,
            session_dir: session_dir.clone(),
            config,
            writers: HashMap::new(),
            last_flush: std::time::Instant::now(),
        };

        // Create initial log files
        writer.ensure_writer(&LogCategory::All)?;
        writer.ensure_writer(&LogCategory::Debug)?;

        info!("Created async log writer in: {}", session_dir.display());
        Ok(writer)
    }

    #[allow(clippy::cognitive_complexity)]
    fn run(mut self) {
        info!("Starting async log writer thread");

        while let Ok(command) = self.command_receiver.recv() {
            match command {
                LogWriterCommand::WriteLog(entry) => {
                    if let Err(e) = self.write_log_entry(&entry) {
                        error!("Failed to write log entry: {}", e);
                    }
                }
                LogWriterCommand::Flush => {
                    if let Err(e) = self.flush_all() {
                        error!("Failed to flush logs: {}", e);
                    }
                }
                LogWriterCommand::Shutdown => {
                    info!("Shutting down async log writer");
                    if let Err(e) = self.close() {
                        error!("Failed to close log writer: {}", e);
                    }
                    break;
                }
            }
        }
    }

    /// Ensure a writer exists for the given log category
    fn ensure_writer(&mut self, category: &LogCategory) -> Result<()> {
        if self.writers.contains_key(&category) {
            return Ok(());
        }

        let file_path = self.get_file_path(&category);

        // Create parent directory if needed
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .with_context(|| format!("Failed to open log file: {}", file_path.display()))?;

        let writer = BufWriter::new(file);
        self.writers.insert(category.clone(), writer);

        debug!("Created log writer for category: {:?}", category);
        Ok(())
    }

    /// Get the full file path for a log category
    fn get_file_path(&self, category: &LogCategory) -> PathBuf {
        let filename = match category {
            LogCategory::All => "all.log".to_string(),
            LogCategory::Debug => "debug.log".to_string(),
            LogCategory::Node(node_id) => format!("node_{}.log", node_id.execution_order()),
        };

        self.session_dir.join(filename)
    }

    /// Write a log entry to the appropriate files
    fn write_log_entry(&mut self, entry: &LogEntry) -> Result<()> {
        let log_line = self.format_log_entry(entry)?;

        // Write to "all" logs
        self.write_to_category(LogCategory::All, &log_line)?;

        // Write to debug logs if it's from main thread
        if entry.node_id == MAIN_THREAD_NODE_ID {
            self.write_to_category(LogCategory::Debug, &log_line)?;
        } else {
            // Write to node-specific log
            let node_category = LogCategory::Node(entry.node_id);
            self.write_to_category(node_category, &log_line)?;
        }

        // Force flush after every write for immediate visibility
        self.flush_all()?;

        Ok(())
    }

    /// Write log line to a specific category
    fn write_to_category(&mut self, category: LogCategory, log_line: &str) -> Result<()> {
        // Ensure writer exists
        self.ensure_writer(&category)?;

        if let Some(writer) = self.writers.get_mut(&category) {
            writeln!(writer, "{log_line}")
                .with_context(|| format!("Failed to write to log category: {category:?}"))?;
        }

        Ok(())
    }

    /// Format a log entry as JSON Lines format
    fn format_log_entry(&self, entry: &LogEntry) -> Result<String> {
        serde_json::to_string(entry).with_context(|| "Failed to serialize log entry")
    }

    /// Flush all writers
    fn flush_all(&mut self) -> Result<()> {
        for (category, writer) in &mut self.writers {
            writer
                .flush()
                .with_context(|| format!("Failed to flush writer for category: {:?}", category))?;
        }
        Ok(())
    }

    /// Close and cleanup
    fn close(&mut self) -> Result<()> {
        info!("Closing async log writer");
        self.flush_all()?;
        self.writers.clear();
        Ok(())
    }
}

/// Configuration for disk-based logging
#[derive(Debug, Clone)]
pub struct DiskLogConfig {
    /// Base directory for log files
    pub base_dir: PathBuf,
    /// Maximum size per log file in bytes (default: 10MB)
    pub max_file_size: u64,
    /// Flush interval in milliseconds (default: 1000ms)
    pub flush_interval_ms: u64,
}

impl DiskLogConfig {
    /// Create a new config with session-based tmp directory
    pub fn new_with_session(session_id: &str) -> Self {
        Self {
            base_dir: PathBuf::from(format!("/tmp/proven/{session_id}")),
            max_file_size: 10 * 1024 * 1024, // 10MB
            flush_interval_ms: 1000,
        }
    }
}

impl Default for DiskLogConfig {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::from("logs"), // Fallback for tests
            max_file_size: 10 * 1024 * 1024, // 10MB
            flush_interval_ms: 1000,
        }
    }
}

/// Log file categories for organizing logs
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum LogCategory {
    /// All logs combined
    All,
    /// Debug/main thread logs only
    Debug,
    /// Logs from a specific node
    Node(NodeId),
}

/// Extract node ID from the current thread name
/// Thread names follow the pattern "node-{execution_order}-{pokemon_id}" (e.g., "node-1-42", "node-2-7")
/// Returns None for non-node threads (main thread, etc.)
fn extract_node_id_from_thread_name() -> Option<NodeId> {
    let thread = std::thread::current();
    let thread_name = thread.name()?;

    // Parse "node-123-45" -> execution_order = 123, pokemon_id = 45
    let node_part = thread_name.strip_prefix("node-")?;
    let mut parts = node_part.split('-');

    let execution_order: u8 = parts.next()?.parse().ok()?;
    let pokemon_id: u8 = parts.next()?.parse().ok()?;

    Some(NodeId::with_values(execution_order, pokemon_id))
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

/// High-performance disk-based log writer with integrated tracing layer
pub struct LogWriter {
    /// Command sender for async log writer
    command_sender: mpsc::Sender<LogWriterCommand>,
    /// Thread handle for async log writer
    writer_thread: Option<thread::JoinHandle<()>>,
    /// Current log level filter
    current_level_filter: Arc<RwLock<LogLevel>>,
    /// Current node filter
    current_node_filter: Arc<RwLock<Option<NodeId>>>,
    /// Current session directory for log reading
    session_dir: Arc<RwLock<PathBuf>>,
}

impl LogWriter {
    /// Create a new disk log writer with default configuration
    pub fn new() -> Result<Self> {
        Self::with_config(DiskLogConfig::default())
    }

    /// Create a new disk log writer with custom configuration
    pub fn with_config(config: DiskLogConfig) -> Result<Self> {
        let session_dir = Arc::new(RwLock::new(config.base_dir.clone()));

        // Create async log writer
        let (command_sender, command_receiver) = mpsc::channel();
        let async_writer = AsyncLogWriter::new(command_receiver, config)?;

        // Start background thread
        let writer_thread = thread::spawn(move || {
            async_writer.run();
        });

        Ok(Self {
            command_sender,
            writer_thread: Some(writer_thread),
            current_level_filter: Arc::new(RwLock::new(LogLevel::Info)),
            current_node_filter: Arc::new(RwLock::new(None)),
            session_dir,
        })
    }

    /// Add a log entry (non-blocking)
    pub fn add_log(&self, entry: LogEntry) {
        if let Err(e) = self.command_sender.send(LogWriterCommand::WriteLog(entry)) {
            error!("Failed to send log entry to async writer: {}", e);
        }
    }

    /// Set the current log level filter
    pub fn set_level_filter(&self, level: LogLevel) {
        *self.current_level_filter.write() = level;
    }

    /// Get the current log level filter
    pub fn get_level_filter(&self) -> LogLevel {
        *self.current_level_filter.read()
    }

    /// Set the current node filter
    pub fn set_node_filter(&self, node_id: Option<NodeId>) {
        *self.current_node_filter.write() = node_id;
    }

    /// Get the current node filter
    pub fn get_node_filter(&self) -> Option<NodeId> {
        *self.current_node_filter.read()
    }

    /// Get the current session directory path
    pub fn get_session_dir(&self) -> Option<PathBuf> {
        let session_dir = self.session_dir.read();
        Some(session_dir.clone())
    }

    /// Flush all pending writes
    pub fn flush(&self) -> Result<()> {
        if let Err(e) = self.command_sender.send(LogWriterCommand::Flush) {
            error!("Failed to send flush command to async writer: {}", e);
            return Err(anyhow::anyhow!("Failed to send flush command: {}", e));
        }
        Ok(())
    }
}

impl Drop for LogWriter {
    fn drop(&mut self) {
        // Send shutdown command to async writer
        if let Err(e) = self.command_sender.send(LogWriterCommand::Shutdown) {
            error!("Failed to send shutdown command during drop: {}", e);
        }

        // Wait for writer thread to finish
        if let Some(handle) = self.writer_thread.take() {
            if let Err(e) = handle.join() {
                error!("Failed to join async writer thread: {:?}", e);
            }
        }
    }
}

impl Default for LogWriter {
    fn default() -> Self {
        Self::new().expect("Failed to create default LogWriter")
    }
}

impl Clone for LogWriter {
    fn clone(&self) -> Self {
        // Share the same session directory and command sender for cloning
        // This ensures all clones write to the same log files
        Self {
            command_sender: self.command_sender.clone(),
            writer_thread: None, // Don't clone the thread handle - only the original owns it
            current_level_filter: Arc::clone(&self.current_level_filter),
            current_node_filter: Arc::clone(&self.current_node_filter),
            session_dir: Arc::clone(&self.session_dir),
        }
    }
}

/// Integrated tracing layer that captures logs and writes them to disk
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

        // Ignore targets containing "async_nats"
        if target.as_deref().unwrap_or_default().contains("async_nats") {
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

        // Send to disk-based log writer (guaranteed delivery)
        self.add_log(log_entry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_config() -> (DiskLogConfig, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = DiskLogConfig {
            base_dir: temp_dir.path().to_path_buf(),
            max_file_size: 1024 * 1024, // 1MB for tests
            flush_interval_ms: 100,
        };
        (config, temp_dir)
    }

    fn create_test_log_entry() -> LogEntry {
        LogEntry {
            node_id: NodeId::new(),
            level: LogLevel::Info,
            message: "Test log message".to_string(),
            timestamp: Utc::now(),
            target: Some("test".to_string()),
        }
    }

    fn reset_node_id_state() {
        #[cfg(test)]
        crate::node_id::NodeId::reset_global_state();
    }

    #[test]
    fn test_writer_creation() {
        let (config, _temp_dir) = create_test_config();
        let writer = LogWriter::with_config(config).expect("Failed to create writer");

        let session_dir = writer.get_session_dir().expect("Failed to get session dir");
        assert!(session_dir.exists());
        assert!(session_dir.join("all.log").exists());
        assert!(session_dir.join("debug.log").exists());
    }

    #[test]
    fn test_log_writer() {
        reset_node_id_state();
        let (config, _temp_dir) = create_test_config();
        let writer = LogWriter::with_config(config).expect("Failed to create writer");

        let entry = create_test_log_entry();
        writer.add_log(entry);

        // Give async writer time to process
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Check that session directory exists
        let session_dir = writer.get_session_dir().expect("Failed to get session dir");
        assert!(session_dir.exists());
        assert!(session_dir.join("all.log").exists());
    }
}
