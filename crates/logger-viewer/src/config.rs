//! Configuration for log viewer

use std::path::PathBuf;
use std::time::Duration;

/// Configuration for the log viewer
#[derive(Debug, Clone)]
pub struct ViewerConfig {
    /// Base directory for log files
    pub log_dir: PathBuf,

    /// File pattern to match (e.g., "*.log", "2024-*.log")
    pub file_pattern: String,

    /// Maximum number of files to keep open
    pub max_open_files: usize,

    /// File discovery interval
    pub discovery_interval: Duration,

    /// Whether to follow symlinks
    pub follow_symlinks: bool,

    /// Buffer size for line reading (in bytes)
    pub buffer_size: usize,

    /// Maximum line length before truncation
    pub max_line_length: usize,

    /// Whether to watch for file changes
    pub watch_files: bool,
}

impl Default for ViewerConfig {
    fn default() -> Self {
        Self {
            log_dir: PathBuf::from("."),
            file_pattern: "*.log".to_string(),
            max_open_files: 100,
            discovery_interval: Duration::from_secs(1),
            follow_symlinks: false,
            buffer_size: 8192,
            max_line_length: 10_000,
            watch_files: true,
        }
    }
}

/// Builder for ViewerConfig
pub struct ViewerConfigBuilder {
    config: ViewerConfig,
}

impl ViewerConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: ViewerConfig::default(),
        }
    }

    pub fn log_dir(mut self, dir: PathBuf) -> Self {
        self.config.log_dir = dir;
        self
    }

    pub fn file_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.config.file_pattern = pattern.into();
        self
    }

    pub fn max_open_files(mut self, max: usize) -> Self {
        self.config.max_open_files = max;
        self
    }

    pub fn discovery_interval(mut self, interval: Duration) -> Self {
        self.config.discovery_interval = interval;
        self
    }

    pub fn follow_symlinks(mut self, follow: bool) -> Self {
        self.config.follow_symlinks = follow;
        self
    }

    pub fn buffer_size(mut self, size: usize) -> Self {
        self.config.buffer_size = size;
        self
    }

    pub fn max_line_length(mut self, length: usize) -> Self {
        self.config.max_line_length = length;
        self
    }

    pub fn watch_files(mut self, watch: bool) -> Self {
        self.config.watch_files = watch;
        self
    }

    pub fn build(self) -> ViewerConfig {
        self.config
    }
}

impl Default for ViewerConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}
