//! High-performance log viewer with memory-mapped file support
//!
//! This crate provides efficient log reading and monitoring capabilities using memory-mapped
//! files for optimal performance. It supports:
//!
//! - Memory-mapped file access for zero-copy reading
//! - Multi-file log reading in chronological order
//! - Real-time file monitoring with inotify/FSEvents
//! - Efficient line indexing and seeking
//! - Configurable filtering and search
//! - Both sync and async APIs

pub mod config;
pub mod error;
pub mod file_index;
pub mod filter;
pub mod mmap_reader;
pub mod monitor;
pub mod multi_reader;
pub mod search;

pub use config::{ViewerConfig, ViewerConfigBuilder};
pub use error::{Error, Result};
pub use file_index::{FileIndex, LineInfo};
pub use filter::{LogEntry, LogFilter};
pub use mmap_reader::MmapLogReader;
pub use monitor::{FileEvent, LogMonitor};
pub use multi_reader::MultiFileReader;
pub use search::{SearchEngine, SearchOptions};

// Re-export key traits
pub trait LogReader: Send + Sync {
    /// Get total number of lines across all files
    fn total_lines(&self) -> Result<usize>;

    /// Read a specific line by global index
    fn read_line(&self, line_index: usize) -> Result<Option<String>>;

    /// Read a range of lines
    fn read_lines(&self, start: usize, count: usize) -> Result<Vec<String>>;

    /// Get file path for a specific line
    fn get_file_for_line(&self, line_index: usize) -> Result<Option<std::path::PathBuf>>;
}

#[cfg(feature = "async")]
pub mod async_api {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;

    pub trait AsyncLogReader: Send + Sync {
        /// Get total number of lines across all files
        fn total_lines(&self) -> Pin<Box<dyn Future<Output = Result<usize>> + Send + '_>>;

        /// Read a specific line by global index
        fn read_line(
            &self,
            line_index: usize,
        ) -> Pin<Box<dyn Future<Output = Result<Option<String>>> + Send + '_>>;

        /// Read a range of lines
        fn read_lines(
            &self,
            start: usize,
            count: usize,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<String>>> + Send + '_>>;
    }
}
