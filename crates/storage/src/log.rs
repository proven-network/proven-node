//! Simplified log storage abstraction
//!
//! This module provides a simplified log storage interface that stores
//! indexed byte sequences without generic metadata types. This design
//! allows for cleaner implementations and better performance by avoiding
//! unnecessary serialization in memory-based storage backends.
//!
//! Key features:
//! - Simple API with only 7 essential methods
//! - No generic types or forced serialization
//! - Efficient range scans and atomic operations
//! - Built-in truncation and compaction support
//! - Native streaming support for large datasets

use async_trait::async_trait;
use bytes::Bytes;
use std::fmt::{Debug, Display};
use tokio_stream::Stream;

/// Result type for storage operations
pub type StorageResult<T> = Result<T, StorageError>;

/// Errors that can occur in storage operations
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Storage backend error
    #[error("Storage backend error: {0}")]
    Backend(String),

    /// Invalid key format
    #[error("Invalid key format: {0}")]
    InvalidKey(String),

    /// Invalid value format
    #[error("Invalid value format: {0}")]
    InvalidValue(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Key not found
    #[error("Key not found: {0}")]
    KeyNotFound(String),

    /// Namespace not found
    #[error("Namespace not found: {0}")]
    NamespaceNotFound(String),

    /// Operation not supported
    #[error("Operation not supported: {0}")]
    NotSupported(String),
}

/// A key in the storage system
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorageKey(pub Bytes);

impl StorageKey {
    /// Create a new storage key from bytes
    pub fn new(key: impl Into<Bytes>) -> Self {
        Self(key.into())
    }

    /// Get the raw bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Display for StorageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match std::str::from_utf8(&self.0) {
            Ok(s) => write!(f, "{s}"),
            Err(_) => write!(f, "0x{}", hex::encode(&self.0)),
        }
    }
}

impl From<&[u8]> for StorageKey {
    fn from(bytes: &[u8]) -> Self {
        Self(Bytes::copy_from_slice(bytes))
    }
}

/// A namespace for organizing data
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StorageNamespace(String);

impl StorageNamespace {
    /// Create a new storage namespace
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Get the namespace as a string
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for StorageNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Simplified log storage trait - just stores indexed byte sequences
#[async_trait]
pub trait LogStorage: Clone + Send + Sync + 'static {
    /// Atomically append entries (one or more)
    async fn append(
        &self,
        namespace: &StorageNamespace,
        entries: Vec<(u64, Bytes)>,
    ) -> StorageResult<()>;

    /// Get the current bounds of the log (first_index, last_index)
    async fn bounds(&self, namespace: &StorageNamespace) -> StorageResult<Option<(u64, u64)>>;

    /// Remove all entries up to and including the given index
    async fn compact_before(&self, namespace: &StorageNamespace, index: u64) -> StorageResult<()>;

    /// Read a range of entries [start, end)
    async fn read_range(
        &self,
        namespace: &StorageNamespace,
        start: u64,
        end: u64,
    ) -> StorageResult<Vec<(u64, Bytes)>>;

    /// Remove all entries after the given index
    async fn truncate_after(&self, namespace: &StorageNamespace, index: u64) -> StorageResult<()>;
}

/// Extended log storage trait that supports deletion of individual entries
/// This trait is separate from LogStorage to ensure consensus logs remain immutable
/// while allowing application streams to support deletion
#[async_trait]
pub trait LogStorageWithDelete: LogStorage {
    /// Delete or tombstone a specific entry at the given index
    /// Returns true if the entry existed and was deleted, false if it didn't exist
    async fn delete_entry(&self, namespace: &StorageNamespace, index: u64) -> StorageResult<bool>;
}

/// Log storage trait that supports streaming reads
/// This trait is optional and allows storage backends to provide optimized streaming
#[async_trait]
pub trait LogStorageStreaming: LogStorage {
    /// Stream entries from a range [start, end)
    ///
    /// Returns a stream of (index, data) pairs. The stream may yield errors inline.
    /// If end is None, streams until the last available entry.
    ///
    /// Implementations should:
    /// - Use backend-specific optimizations (e.g., RocksDB iterators)
    /// - Handle concurrent modifications gracefully
    /// - Yield entries in index order
    async fn stream_range(
        &self,
        namespace: &StorageNamespace,
        start: u64,
        end: Option<u64>,
    ) -> StorageResult<Box<dyn Stream<Item = StorageResult<(u64, Bytes)>> + Send + Unpin>>;
}

/// Implement LogStorage for Arc<T> where T: LogStorage
#[async_trait]
impl<T: LogStorage> LogStorage for std::sync::Arc<T> {
    async fn append(
        &self,
        namespace: &StorageNamespace,
        entries: Vec<(u64, Bytes)>,
    ) -> StorageResult<()> {
        (**self).append(namespace, entries).await
    }

    async fn bounds(&self, namespace: &StorageNamespace) -> StorageResult<Option<(u64, u64)>> {
        (**self).bounds(namespace).await
    }

    async fn compact_before(&self, namespace: &StorageNamespace, index: u64) -> StorageResult<()> {
        (**self).compact_before(namespace, index).await
    }

    async fn read_range(
        &self,
        namespace: &StorageNamespace,
        start: u64,
        end: u64,
    ) -> StorageResult<Vec<(u64, Bytes)>> {
        (**self).read_range(namespace, start, end).await
    }

    async fn truncate_after(&self, namespace: &StorageNamespace, index: u64) -> StorageResult<()> {
        (**self).truncate_after(namespace, index).await
    }
}

/// Implement LogStorageWithDelete for Arc<T> where T: LogStorageWithDelete
#[async_trait]
impl<T: LogStorageWithDelete> LogStorageWithDelete for std::sync::Arc<T> {
    async fn delete_entry(&self, namespace: &StorageNamespace, index: u64) -> StorageResult<bool> {
        (**self).delete_entry(namespace, index).await
    }
}

/// Implement LogStorageStreaming for Arc<T> where T: LogStorageStreaming
#[async_trait]
impl<T: LogStorageStreaming> LogStorageStreaming for std::sync::Arc<T> {
    async fn stream_range(
        &self,
        namespace: &StorageNamespace,
        start: u64,
        end: Option<u64>,
    ) -> StorageResult<Box<dyn Stream<Item = StorageResult<(u64, Bytes)>> + Send + Unpin>> {
        (**self).stream_range(namespace, start, end).await
    }
}
