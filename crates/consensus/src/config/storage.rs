//! Storage configuration types

use std::path::PathBuf;

/// Storage configuration options
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum StorageConfig {
    /// In-memory storage
    Memory,
    /// RocksDB persistent storage
    RocksDB {
        /// Database path
        path: PathBuf,
    },
}
