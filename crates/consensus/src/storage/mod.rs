//! Storage adaptor layer for consensus
//!
//! This module provides a unified abstraction over different storage backends,
//! handling low-level persistence, WALs, encryption, and other storage concerns.
//! The adaptors provide read/write access to "logs" (sequential data streams)
//! which can represent Raft logs, stream messages, or other ordered data.

pub mod adaptors;
pub mod generic;
pub mod log;
pub mod traits;
pub mod types;

pub use log::{CompactionResult, LogEntry, LogState, LogStorage as EnhancedLogStorage, keys};
pub use traits::{LogStorage, SnapshotStorage, StorageEngine};
pub use types::{
    StorageError, StorageIterator, StorageKey, StorageNamespace, StorageResult, StorageValue,
    WriteBatch,
};

// Re-export specific adaptors
pub use adaptors::{memory::MemoryStorage, rocksdb::RocksDBStorage};
