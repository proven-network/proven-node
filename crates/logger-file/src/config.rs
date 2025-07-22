//! Configuration for file-based logging

use std::path::PathBuf;
use std::time::Duration;

/// Rotation policy for log files
#[derive(Debug, Clone)]
pub enum RotationPolicy {
    /// Rotate based on file size
    Size {
        /// Maximum size in bytes before rotation
        max_bytes: u64,
    },
    /// Rotate based on time
    Time {
        /// Duration before rotation
        interval: Duration,
    },
    /// Rotate based on both size and time (whichever comes first)
    SizeOrTime {
        /// Maximum size in bytes before rotation
        max_bytes: u64,
        /// Duration before rotation
        interval: Duration,
    },
    /// Never rotate
    Never,
}

impl Default for RotationPolicy {
    fn default() -> Self {
        // Default to 100MB rotation
        Self::Size {
            max_bytes: 100 * 1024 * 1024,
        }
    }
}

/// Configuration for file-based logger
#[derive(Debug, Clone)]
pub struct FileLoggerConfig {
    /// Base directory for log files
    pub log_dir: PathBuf,
    /// File name prefix
    pub file_prefix: String,
    /// File extension
    pub file_extension: String,
    /// Rotation policy
    pub rotation_policy: RotationPolicy,
    /// Maximum number of rotated files to keep
    pub max_files: Option<usize>,
    /// Whether to create symlinks to latest files
    pub create_symlinks: bool,
    /// Buffer size for async writes
    pub buffer_size: usize,
    /// Flush interval for buffered writes
    pub flush_interval: Duration,
    /// Whether to compress rotated files
    pub compress_rotated: bool,
}

impl Default for FileLoggerConfig {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from("logs"),
            file_prefix: String::from("app"),
            file_extension: String::from("log"),
            rotation_policy: RotationPolicy::default(),
            max_files: Some(10),
            create_symlinks: true,
            buffer_size: 8192,
            flush_interval: Duration::from_millis(100),
            compress_rotated: false,
        }
    }
}

/// Builder for FileLoggerConfig
pub struct FileLoggerConfigBuilder {
    config: FileLoggerConfig,
}

impl FileLoggerConfigBuilder {
    /// Create a new builder with default configuration
    pub fn new() -> Self {
        Self {
            config: FileLoggerConfig::default(),
        }
    }

    /// Set the log directory
    pub fn log_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.config.log_dir = dir.into();
        self
    }

    /// Set the file prefix
    pub fn file_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.config.file_prefix = prefix.into();
        self
    }

    /// Set the file extension
    pub fn file_extension(mut self, ext: impl Into<String>) -> Self {
        self.config.file_extension = ext.into();
        self
    }

    /// Set the rotation policy
    pub fn rotation_policy(mut self, policy: RotationPolicy) -> Self {
        self.config.rotation_policy = policy;
        self
    }

    /// Set the maximum number of files to keep
    pub fn max_files(mut self, max: Option<usize>) -> Self {
        self.config.max_files = max;
        self
    }

    /// Enable or disable symlink creation
    pub fn create_symlinks(mut self, create: bool) -> Self {
        self.config.create_symlinks = create;
        self
    }

    /// Set the buffer size
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.config.buffer_size = size;
        self
    }

    /// Set the flush interval
    pub fn flush_interval(mut self, interval: Duration) -> Self {
        self.config.flush_interval = interval;
        self
    }

    /// Enable or disable compression of rotated files
    pub fn compress_rotated(mut self, compress: bool) -> Self {
        self.config.compress_rotated = compress;
        self
    }

    /// Build the configuration
    pub fn build(self) -> FileLoggerConfig {
        self.config
    }
}

impl Default for FileLoggerConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}
