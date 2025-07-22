//! Disk-based log writing system using proven-logger-file

use crate::logs_categorizer::{AllLogsCategorizer, TuiLogCategorizer};
use crate::logs_formatter::TuiLogFormatter;
use crate::messages::{LogLevel, TuiNodeId};
use anyhow::Result;
use parking_lot::RwLock;
use proven_logger::{Logger, Record};
use proven_logger_file::{FileLogger, FileLoggerConfigBuilder, RotationPolicy};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::runtime::{Handle, Runtime};

/// Global runtime for file logger tasks
static LOGGER_RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// High-performance disk-based log writer
pub struct LogWriter {
    /// File logger for "all" logs
    all_logger: Arc<FileLogger>,
    /// File logger for categorized logs (`debug/node_X`)
    categorized_logger: Arc<FileLogger>,
    /// Tokio runtime handle for async operations
    runtime_handle: Handle,
    /// Current log level filter
    current_level_filter: Arc<RwLock<LogLevel>>,
    /// Current node filter
    current_node_filter: Arc<RwLock<Option<TuiNodeId>>>,
    /// Current session directory for log reading
    session_dir: Arc<RwLock<PathBuf>>,
}

impl LogWriter {
    /// Create symlinks at the root level for compatibility with old log reader
    fn create_root_symlinks(base_dir: &Path) -> Result<()> {
        // We'll create these symlinks after the first log files are created
        // For now, just ensure the directories exist
        std::fs::create_dir_all(base_dir.join("all"))?;
        std::fs::create_dir_all(base_dir.join("debug"))?;
        Ok(())
    }

    /// Create a new disk log writer with default configuration (async)
    ///
    /// # Errors
    ///
    /// Returns an error if the log directories cannot be created or if the file loggers fail to initialize
    pub async fn new(base_dir: &Path) -> Result<Self> {
        let session_dir = Arc::new(RwLock::new(base_dir.to_path_buf()));

        // Get current runtime handle
        let runtime_handle = Handle::current();

        // Create logger configurations
        let all_config = FileLoggerConfigBuilder::new()
            .log_dir(base_dir.join("all"))
            .file_prefix("") // No prefix to match old format
            .file_extension("log")
            .buffer_size(1000)
            .flush_interval(Duration::from_millis(100))
            .rotation_policy(RotationPolicy::Size {
                max_bytes: 1024 * 1024,
            }) // 1MB
            .create_symlinks(false) // Don't create symlinks in subdirectories
            .build();

        let categorized_config = FileLoggerConfigBuilder::new()
            .log_dir(base_dir)
            .file_prefix("") // No prefix to match old format
            .file_extension("log")
            .buffer_size(1000)
            .flush_interval(Duration::from_millis(100))
            .rotation_policy(RotationPolicy::Size {
                max_bytes: 1024 * 1024,
            }) // 1MB
            .create_symlinks(false) // Don't create symlinks in subdirectories
            .build();

        // Create file loggers
        let all_logger = FileLogger::new(
            all_config,
            Arc::new(TuiLogFormatter),
            Arc::new(AllLogsCategorizer),
        )
        .await?;

        let categorized_logger = FileLogger::new(
            categorized_config,
            Arc::new(TuiLogFormatter),
            Arc::new(TuiLogCategorizer),
        )
        .await?;

        // Create root-level symlinks for compatibility
        Self::create_root_symlinks(base_dir)?;

        Ok(Self {
            all_logger: Arc::new(all_logger),
            categorized_logger: Arc::new(categorized_logger),
            runtime_handle,
            current_level_filter: Arc::new(RwLock::new(LogLevel::Info)),
            current_node_filter: Arc::new(RwLock::new(None)),
            session_dir,
        })
    }

    /// Create a new disk log writer synchronously (for compatibility)
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime cannot be created or if the async initialization fails
    ///
    /// # Panics
    ///
    /// Panics if the logger runtime fails to build, which should only happen in extreme cases
    /// such as system resource exhaustion
    pub fn new_sync(base_dir: &Path) -> Result<Self> {
        // Get or create the global logger runtime
        let runtime = LOGGER_RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("logger")
                .enable_all()
                .build()
                .expect("Failed to create logger runtime")
        });

        runtime.block_on(Self::new(base_dir))
    }

    /// Get the current session directory path
    #[must_use]
    pub fn get_session_dir(&self) -> PathBuf {
        self.session_dir.read().clone()
    }

    /// Internal method to log a record
    fn log_record(&self, record: Record) {
        // Log to both "all" and categorized loggers
        self.all_logger.log(record.clone());
        self.categorized_logger.log(record);
    }
}

impl Logger for LogWriter {
    fn log(&self, record: Record) {
        self.log_record(record);
    }

    fn flush(&self) {
        // FileLogger handles flushing internally
        self.all_logger.flush();
        self.categorized_logger.flush();
    }

    fn with_context(&self, _context: proven_logger::Context) -> Arc<dyn Logger> {
        // For now, just return self since we don't modify context
        // In the future, we could create a wrapper that includes context
        Arc::new(self.clone())
    }
}

impl Clone for LogWriter {
    fn clone(&self) -> Self {
        Self {
            all_logger: Arc::clone(&self.all_logger),
            categorized_logger: Arc::clone(&self.categorized_logger),
            runtime_handle: self.runtime_handle.clone(),
            current_level_filter: Arc::clone(&self.current_level_filter),
            current_node_filter: Arc::clone(&self.current_node_filter),
            session_dir: Arc::clone(&self.session_dir),
        }
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

    #[tokio::test]
    async fn test_writer_creation() {
        let base_dir = create_temp_dir();
        let writer = LogWriter::new(&base_dir)
            .await
            .expect("Failed to create writer");

        let session_dir = writer.get_session_dir();
        assert!(session_dir.exists());
        assert!(session_dir.join("all").exists());
    }

    #[tokio::test]
    async fn test_log_writing() {
        let base_dir = create_temp_dir();
        let writer = LogWriter::new(&base_dir)
            .await
            .expect("Failed to create writer");

        // Log a test message using the internal method
        let record = proven_logger::Record::new(proven_logger::Level::Info, "Test message")
            .with_target("test");
        writer.log_record(record);

        // Flush the logger
        writer.all_logger.flush();
        writer.categorized_logger.flush();

        // Give the async writer time to process
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

        // Check that files were created
        let all_dir = base_dir.join("all");
        assert!(all_dir.exists());

        // Check for log files
        let entries: Vec<_> = std::fs::read_dir(&all_dir)
            .expect("Failed to read all directory")
            .collect();
        assert!(!entries.is_empty(), "No log files created in all directory");

        // Read the log file and verify content
        for entry in entries.into_iter().flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("log")
                && path.file_name().and_then(|s| s.to_str()).is_some_and(|s| {
                    // Check if filename matches expected pattern YYYY-MM-DD_HH-MM-SS.log
                    s.len() == 23
                        && std::path::Path::new(s)
                            .extension()
                            .is_some_and(|ext| ext.eq_ignore_ascii_case("log"))
                        && s.chars().nth(4) == Some('-')
                })
            {
                let content = std::fs::read_to_string(&path).expect("Failed to read log file");
                println!("Log file path: {path:?}");
                println!("Log file content:\n{content}");
                assert!(
                    content.contains("Test message"),
                    "Log message not found in file"
                );
                // The log format is JSON, so check for the level in JSON format
                assert!(
                    content.contains("\"level\":\"Info\"")
                        || content.contains("\"level\":\"INFO\""),
                    "Log level not found in file"
                );
                break;
            }
        }
    }
}
