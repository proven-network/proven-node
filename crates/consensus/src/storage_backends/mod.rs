//! Storage adaptor layer for consensus
//!
//! This module provides a unified abstraction over different storage backends,
//! handling low-level persistence, WALs, encryption, and other storage concerns.
//! The adaptors provide read/write access to "logs" (sequential data streams)
//! which can represent Raft logs, stream messages, or other ordered data.

pub mod log;

// Backends
pub mod memory;
pub mod rocksdb;
#[cfg(feature = "s3")]
pub mod s3;

pub mod traits;
pub mod types;

pub use log::{LogEntry, keys};
pub use traits::StorageEngine;
pub use types::{
    StorageIterator, StorageKey, StorageNamespace, StorageResult, StorageValue, WriteBatch,
};
