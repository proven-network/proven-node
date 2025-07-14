//! Disk-based log writing system with session management

use crate::messages::{LogEntry, LogLevel, MAIN_THREAD_NODE_ID, NodeId};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::fmt::Write;
use std::sync::Arc;
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write as IoWrite},
    path::{Path, PathBuf},
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
    Shutdown,
    WriteLog(LogEntry),
}

/// File size limit for log rotation (1MB)
const MAX_FILE_SIZE: u64 = 1024 * 1024;

/// Async log writer that runs in background thread
struct AsyncLogWriter {
    command_receiver: mpsc::Receiver<LogWriterCommand>,
    session_dir: PathBuf,
    writers: HashMap<LogCategory, BufWriter<File>>,
    /// Track current file sizes for rotation
    file_sizes: HashMap<LogCategory, u64>,
    /// Track current file paths for each category
    current_files: HashMap<LogCategory, PathBuf>,
}

impl AsyncLogWriter {
    fn new(command_receiver: mpsc::Receiver<LogWriterCommand>, base_dir: &PathBuf) -> Result<Self> {
        fs::create_dir_all(base_dir).with_context(|| {
            format!("Failed to create session directory: {}", base_dir.display())
        })?;

        let mut writer = Self {
            command_receiver,
            session_dir: base_dir.clone(),
            writers: HashMap::new(),
            file_sizes: HashMap::new(),
            current_files: HashMap::new(),
        };

        // Create initial log files and directory structure
        writer.ensure_writer(&LogCategory::All)?;
        writer.ensure_writer(&LogCategory::Debug)?;

        info!("Created async log writer in: {}", base_dir.display());
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
        if self.writers.contains_key(category) {
            return Ok(());
        }

        self.rotate_file(category)?;
        Ok(())
    }

    /// Get the directory path for a log category
    fn get_category_dir(&self, category: &LogCategory) -> PathBuf {
        let dir_name = match category {
            LogCategory::All => "all",
            LogCategory::Debug => "debug",
            LogCategory::Node(node_id) => {
                return self
                    .session_dir
                    .join(format!("node_{}", node_id.execution_order()));
            }
        };
        self.session_dir.join(dir_name)
    }

    /// Get the current symlink path for a log category (legacy location)
    fn get_symlink_path(&self, category: &LogCategory) -> PathBuf {
        let filename = match category {
            LogCategory::All => "all.log",
            LogCategory::Debug => "debug.log",
            LogCategory::Node(node_id) => {
                return self
                    .session_dir
                    .join(format!("node_{}.log", node_id.execution_order()));
            }
        };
        self.session_dir.join(filename)
    }

    /// Generate timestamped filename
    fn generate_timestamped_filename(timestamp: DateTime<Utc>) -> String {
        format!("{}.log", timestamp.format("%Y-%m-%d_%H-%M-%S"))
    }

    /// Create a new log file for the given category
    fn rotate_file(&mut self, category: &LogCategory) -> Result<()> {
        // Close existing writer if any
        if let Some(mut writer) = self.writers.remove(category) {
            writer.flush()?;
        }

        // Create category directory
        let category_dir = self.get_category_dir(category);
        fs::create_dir_all(&category_dir).with_context(|| {
            format!(
                "Failed to create category directory: {}",
                category_dir.display()
            )
        })?;

        // Generate timestamped filename
        let timestamp = Utc::now();
        let filename = Self::generate_timestamped_filename(timestamp);
        let file_path = category_dir.join(&filename);

        // Create new file
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .with_context(|| format!("Failed to create log file: {}", file_path.display()))?;

        let writer = BufWriter::new(file);
        self.writers.insert(category.clone(), writer);
        self.file_sizes.insert(category.clone(), 0);
        self.current_files
            .insert(category.clone(), file_path.clone());

        // Create/update symlink
        self.update_symlink(category, &file_path)?;

        debug!(
            "Rotated log file for category: {:?} -> {}",
            category,
            file_path.display()
        );
        Ok(())
    }

    /// Update symlink to point to current file
    fn update_symlink(&self, category: &LogCategory, target_path: &Path) -> Result<()> {
        let symlink_path = self.get_symlink_path(category);

        // Remove existing symlink if it exists
        if symlink_path.exists() || symlink_path.is_symlink() {
            fs::remove_file(&symlink_path).with_context(|| {
                format!("Failed to remove old symlink: {}", symlink_path.display())
            })?;
        }

        // Create relative path for symlink
        let relative_target = target_path
            .strip_prefix(&self.session_dir)
            .with_context(|| "Failed to create relative path for symlink")?;

        // Create symlink
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(relative_target, &symlink_path)
                .with_context(|| format!("Failed to create symlink: {}", symlink_path.display()))?;
        }

        #[cfg(windows)]
        {
            std::os::windows::fs::symlink_file(relative_target, &symlink_path)
                .with_context(|| format!("Failed to create symlink: {}", symlink_path.display()))?;
        }

        debug!(
            "Updated symlink: {} -> {}",
            symlink_path.display(),
            relative_target.display()
        );
        Ok(())
    }

    /// Check if file needs rotation and rotate if necessary
    fn check_and_rotate_if_needed(&mut self, category: &LogCategory) -> Result<()> {
        let current_size = self.file_sizes.get(category).copied().unwrap_or(0);

        if current_size >= MAX_FILE_SIZE {
            debug!(
                "File size limit reached for category {:?}, rotating",
                category
            );
            self.rotate_file(category)?;
        }

        Ok(())
    }

    /// Write a log entry to the appropriate files
    fn write_log_entry(&mut self, entry: &LogEntry) -> Result<()> {
        let log_line = Self::format_log_entry(entry)?;
        let log_line_bytes = log_line.len() as u64 + 1; // +1 for newline

        // Write to "all" logs
        self.check_and_rotate_if_needed(&LogCategory::All)?;
        self.write_to_category(&LogCategory::All, &log_line, log_line_bytes)?;

        // Write to debug logs if it's from main thread
        if entry.node_id == MAIN_THREAD_NODE_ID {
            self.check_and_rotate_if_needed(&LogCategory::Debug)?;
            self.write_to_category(&LogCategory::Debug, &log_line, log_line_bytes)?;
        } else {
            // Write to node-specific log
            let node_category = LogCategory::Node(entry.node_id);
            self.check_and_rotate_if_needed(&node_category)?;
            self.write_to_category(&node_category, &log_line, log_line_bytes)?;
        }

        // Force flush after every write for immediate visibility
        self.flush_all()?;

        Ok(())
    }

    /// Write log line to a specific category
    fn write_to_category(
        &mut self,
        category: &LogCategory,
        log_line: &str,
        line_bytes: u64,
    ) -> Result<()> {
        // Ensure writer exists
        self.ensure_writer(category)?;

        if let Some(writer) = self.writers.get_mut(category) {
            writeln!(writer, "{log_line}")
                .with_context(|| format!("Failed to write to log category: {category:?}"))?;

            // Update file size
            let current_size = self.file_sizes.get(category).copied().unwrap_or(0);
            self.file_sizes
                .insert(category.clone(), current_size + line_bytes);
        }

        Ok(())
    }

    /// Format a log entry as JSON Lines format
    fn format_log_entry(entry: &LogEntry) -> Result<String> {
        serde_json::to_string(entry).with_context(|| "Failed to serialize log entry")
    }

    /// Flush all writers
    fn flush_all(&mut self) -> Result<()> {
        for (category, writer) in &mut self.writers {
            writer
                .flush()
                .with_context(|| format!("Failed to flush writer for category: {category:?}"))?;
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
    pub fn new(base_dir: &PathBuf) -> Result<Self> {
        let session_dir = Arc::new(RwLock::new(base_dir.clone()));

        // Create async log writer
        let (command_sender, command_receiver) = mpsc::channel();
        let async_writer = AsyncLogWriter::new(command_receiver, base_dir)?;

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

    /// Get the current session directory path
    pub fn get_session_dir(&self) -> PathBuf {
        self.session_dir.read().clone()
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

        // Ignore targets from certain sources
        if let Some(target_str) = target.as_deref()
            && (target_str.starts_with("async_nats")
                || target_str.starts_with("openraft")
                || target_str.starts_with("hyper_util")
                || target_str.starts_with("swc_ecma_transforms_base")
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

        // Send to disk-based log writer (guaranteed delivery)
        self.add_log(log_entry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_temp_dir() -> PathBuf {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        temp_dir.path().to_path_buf()
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
        let base_dir = create_temp_dir();
        let writer = LogWriter::new(&base_dir).expect("Failed to create writer");

        let session_dir = writer.get_session_dir();
        assert!(session_dir.exists());
        assert!(session_dir.join("all.log").exists());
        assert!(session_dir.join("debug.log").exists());
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

        // Check that session directory exists
        let session_dir = writer.get_session_dir();
        assert!(session_dir.exists());
        assert!(session_dir.join("all.log").exists());
    }
}
