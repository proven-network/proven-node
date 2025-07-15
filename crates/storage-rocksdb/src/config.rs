//! RocksDB configuration

use std::path::PathBuf;

/// Configuration for RocksDB storage
#[derive(Debug, Clone)]
pub struct RocksDBConfig {
    /// Path to the RocksDB database
    pub path: PathBuf,
}

impl RocksDBConfig {
    /// Create a new configuration with the given path
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}
