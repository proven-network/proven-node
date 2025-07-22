//! Async log writer implementation with rotation support

use crate::{
    categorizer::LogCategorizer,
    config::{FileLoggerConfig, RotationPolicy},
    error::{Error, Result},
    formatter::LogFormatter,
};
use chrono::{DateTime, Utc};
use proven_logger::{Context, Level, Logger, Record};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc,
    task::JoinHandle,
    time,
};

/// Message type for the async writer
enum WriterMessage {
    Log(Vec<u8>, Option<PathBuf>),
    Flush,
    Shutdown,
}

/// Information about an active log file
struct LogFileInfo {
    /// The file handle
    writer: BufWriter<File>,
    /// Path to the current file
    #[allow(dead_code)]
    path: PathBuf,
    /// Current size in bytes
    size: u64,
    /// When the file was created
    created_at: DateTime<Utc>,
    /// Last write time
    last_write: Instant,
}

/// Async log writer that handles file I/O in a background task
pub struct AsyncLogWriter {
    /// Configuration
    #[allow(dead_code)]
    config: FileLoggerConfig,
    /// Channel for sending logs to the writer task
    sender: mpsc::Sender<WriterMessage>,
    /// Handle to the writer task
    _writer_handle: JoinHandle<()>,
}

impl AsyncLogWriter {
    /// Create a new async log writer
    pub async fn new(
        config: FileLoggerConfig,
        categorizer: Arc<dyn LogCategorizer>,
    ) -> Result<Self> {
        // Create log directory if it doesn't exist
        fs::create_dir_all(&config.log_dir).map_err(|e| Error::CreateDirectory {
            path: config.log_dir.clone(),
            source: e,
        })?;

        let (sender, receiver) = mpsc::channel(config.buffer_size);

        let writer_config = config.clone();
        let writer_handle = tokio::spawn(async move {
            if let Err(e) = writer_task(receiver, writer_config, categorizer).await {
                eprintln!("Log writer task failed: {e}");
            }
        });

        Ok(Self {
            config,
            sender,
            _writer_handle: writer_handle,
        })
    }

    /// Write a log entry
    pub async fn write(&self, bytes: Vec<u8>, category: Option<PathBuf>) -> Result<()> {
        self.sender
            .send(WriterMessage::Log(bytes, category))
            .await
            .map_err(|_| Error::ChannelClosed)
    }

    /// Flush all pending writes
    pub async fn flush(&self) -> Result<()> {
        self.sender
            .send(WriterMessage::Flush)
            .await
            .map_err(|_| Error::ChannelClosed)
    }

    /// Shutdown the writer
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.sender.send(WriterMessage::Shutdown).await;
        Ok(())
    }
}

/// The background writer task
async fn writer_task(
    mut receiver: mpsc::Receiver<WriterMessage>,
    config: FileLoggerConfig,
    _categorizer: Arc<dyn LogCategorizer>,
) -> Result<()> {
    // Writer task started
    let mut files: HashMap<PathBuf, LogFileInfo> = HashMap::new();
    let mut flush_interval = time::interval(config.flush_interval);
    flush_interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            Some(msg) = receiver.recv() => {
                match msg {
                    WriterMessage::Log(bytes, category) => {
                        // Received log message
                        write_log(&config, &mut files, bytes, category).await?;
                    }
                    WriterMessage::Flush => {
                        // Flushing logs
                        flush_all(&mut files).await?;
                    }
                    WriterMessage::Shutdown => {
                        // Shutting down writer
                        flush_all(&mut files).await?;
                        break;
                    }
                }
            }
            _ = flush_interval.tick() => {
                flush_all(&mut files).await?;
                check_rotation(&config, &mut files).await?;
            }
            else => break,
        }
    }

    Ok(())
}

/// Write a log entry to the appropriate file
async fn write_log(
    config: &FileLoggerConfig,
    files: &mut HashMap<PathBuf, LogFileInfo>,
    bytes: Vec<u8>,
    category: Option<PathBuf>,
) -> Result<()> {
    let key = category.unwrap_or_default();
    let bytes_len = bytes.len() as u64;

    // Get or create the file info
    if !files.contains_key(&key) {
        let file_info = create_log_file(config, &key).await?;
        files.insert(key.clone(), file_info);
    }

    let file_info = files.get_mut(&key).unwrap();

    // Write the log
    file_info.writer.write_all(&bytes).await?;
    file_info.size += bytes_len;
    file_info.last_write = Instant::now();

    Ok(())
}

/// Create a new log file
async fn create_log_file(config: &FileLoggerConfig, category: &Path) -> Result<LogFileInfo> {
    let now = Utc::now();
    let timestamp = now.format("%Y%m%d_%H%M%S");

    let mut file_path = config.log_dir.clone();
    if !category.as_os_str().is_empty() {
        file_path.push(category);
        fs::create_dir_all(&file_path).map_err(|e| Error::CreateDirectory {
            path: file_path.clone(),
            source: e,
        })?;
    }

    let file_name = format!(
        "{}_{}.{}",
        config.file_prefix, timestamp, config.file_extension
    );
    file_path.push(&file_name);

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file_path)
        .await?;

    // Create symlink to latest file
    if config.create_symlinks {
        let mut symlink_path = config.log_dir.clone();
        if !category.as_os_str().is_empty() {
            symlink_path.push(category);
        }
        symlink_path.push(format!("{}.{}", config.file_prefix, config.file_extension));

        let _ = fs::remove_file(&symlink_path);
        #[cfg(unix)]
        let _ = std::os::unix::fs::symlink(&file_path, &symlink_path);
    }

    Ok(LogFileInfo {
        writer: BufWriter::new(file),
        path: file_path,
        size: 0,
        created_at: now,
        last_write: Instant::now(),
    })
}

/// Flush all open files
async fn flush_all(files: &mut HashMap<PathBuf, LogFileInfo>) -> Result<()> {
    for (_, file_info) in files.iter_mut() {
        file_info.writer.flush().await?;
    }
    Ok(())
}

/// Check if any files need rotation
async fn check_rotation(
    config: &FileLoggerConfig,
    files: &mut HashMap<PathBuf, LogFileInfo>,
) -> Result<()> {
    let now = Utc::now();
    let mut to_rotate = Vec::new();

    for (category, file_info) in files.iter() {
        let should_rotate = match &config.rotation_policy {
            RotationPolicy::Size { max_bytes } => file_info.size >= *max_bytes,
            RotationPolicy::Time { interval } => {
                let age = now.signed_duration_since(file_info.created_at);
                age >= chrono::Duration::from_std(*interval).unwrap_or(chrono::Duration::days(1))
            }
            RotationPolicy::SizeOrTime {
                max_bytes,
                interval,
            } => {
                let age = now.signed_duration_since(file_info.created_at);
                file_info.size >= *max_bytes
                    || age
                        >= chrono::Duration::from_std(*interval)
                            .unwrap_or(chrono::Duration::days(1))
            }
            RotationPolicy::Never => false,
        };

        if should_rotate {
            to_rotate.push(category.clone());
        }
    }

    // Rotate files that need it
    for category in to_rotate {
        if let Some(mut file_info) = files.remove(&category) {
            file_info.writer.flush().await?;

            // Compress if configured
            if config.compress_rotated {
                // TODO: Implement compression
            }

            // Create new file
            let new_file_info = create_log_file(config, &category).await?;
            files.insert(category, new_file_info);
        }
    }

    // Clean up old files if needed
    if let Some(max_files) = config.max_files {
        cleanup_old_files(config, max_files).await?;
    }

    Ok(())
}

/// Clean up old rotated files
async fn cleanup_old_files(_config: &FileLoggerConfig, _max_files: usize) -> Result<()> {
    // TODO: Implement cleanup logic
    Ok(())
}

/// File-based logger implementation
pub struct FileLogger {
    /// The async writer
    writer: Arc<AsyncLogWriter>,
    /// Log formatter
    formatter: Arc<dyn LogFormatter>,
    /// Log categorizer
    categorizer: Arc<dyn LogCategorizer>,
    /// Minimum log level
    min_level: Level,
    /// Logger context
    context: Context,
    /// Direct channel sender for sync contexts
    sender: mpsc::Sender<WriterMessage>,
}

impl FileLogger {
    /// Create a new file logger
    pub async fn new(
        config: FileLoggerConfig,
        formatter: Arc<dyn LogFormatter>,
        categorizer: Arc<dyn LogCategorizer>,
    ) -> Result<Self> {
        let writer = Arc::new(AsyncLogWriter::new(config, categorizer.clone()).await?);
        let sender = writer.sender.clone();

        Ok(Self {
            writer,
            formatter,
            categorizer,
            min_level: Level::Trace,
            context: Context::new("file-logger"),
            sender,
        })
    }

    /// Set the minimum log level
    pub fn with_level(mut self, level: Level) -> Self {
        self.min_level = level;
        self
    }

    /// Set the logger context
    pub fn with_context(mut self, context: Context) -> Self {
        self.context = context;
        self
    }
}

impl Logger for FileLogger {
    fn is_enabled(&self, level: Level) -> bool {
        level >= self.min_level
    }

    fn log(&self, record: Record) {
        if !self.is_enabled(record.level) {
            return;
        }

        // Create a new record with context if not already set
        let record_with_context = if record.context.is_none() {
            Record::new(record.level, record.message)
                .with_target(record.target)
                .with_context(&self.context)
        } else {
            record
        };

        // Format the record
        let bytes = match self.formatter.format(&record_with_context) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("Failed to format log record: {e}");
                return;
            }
        };

        // Get category
        let category = self.categorizer.categorize(&record_with_context);

        // Send to writer using try_send for non-blocking operation
        if let Err(e) = self.sender.try_send(WriterMessage::Log(bytes, category)) {
            eprintln!("Failed to send log to writer: {e}");
        }
    }

    fn flush(&self) {
        let _ = self.sender.try_send(WriterMessage::Flush);
    }

    fn with_context(&self, context: Context) -> Arc<dyn Logger> {
        Arc::new(Self {
            writer: self.writer.clone(),
            formatter: self.formatter.clone(),
            categorizer: self.categorizer.clone(),
            min_level: self.min_level,
            context: self.context.merge(&context),
            sender: self.sender.clone(),
        })
    }
}
