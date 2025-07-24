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
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display},
    num::NonZeroU64,
    sync::Arc,
};
use tokio_stream::Stream;

/// Log index type that enforces 1-based indexing
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LogIndex(NonZeroU64);

impl LogIndex {
    /// Create a new LogIndex from a u64 value
    /// Returns None if the value is 0
    pub fn new(index: u64) -> Option<Self> {
        NonZeroU64::new(index).map(Self)
    }

    /// Get the inner u64 value
    pub fn get(&self) -> u64 {
        self.0.get()
    }

    /// Get the inner NonZeroU64 value
    pub fn inner(&self) -> NonZeroU64 {
        self.0
    }

    /// Increment the log index by 1
    pub fn next(&self) -> Self {
        // Safe because we're adding 1 to a non-zero value
        Self(NonZeroU64::new(self.0.get() + 1).unwrap())
    }

    /// Decrement the log index by 1
    /// Returns None if this would result in 0
    pub fn prev(&self) -> Option<Self> {
        NonZeroU64::new(self.0.get() - 1).map(Self)
    }
}

impl Display for LogIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// Arithmetic implementations for LogIndex

impl std::ops::Add<u64> for LogIndex {
    type Output = Option<Self>;

    fn add(self, rhs: u64) -> Self::Output {
        self.0.get().checked_add(rhs).and_then(LogIndex::new)
    }
}

impl std::ops::Sub<u64> for LogIndex {
    type Output = Option<Self>;

    fn sub(self, rhs: u64) -> Self::Output {
        self.0.get().checked_sub(rhs).and_then(LogIndex::new)
    }
}

impl std::ops::Sub for LogIndex {
    type Output = u64;

    /// Returns the distance between two log indices
    fn sub(self, rhs: Self) -> Self::Output {
        self.0.get().saturating_sub(rhs.0.get())
    }
}

// Convenience methods for checked arithmetic
impl LogIndex {
    /// Add a u64 to this index, returning None on overflow or if result would be 0
    pub fn checked_add(self, n: u64) -> Option<Self> {
        self + n
    }

    /// Subtract a u64 from this index, returning None on underflow or if result would be 0
    pub fn checked_sub(self, n: u64) -> Option<Self> {
        self - n
    }

    /// Saturating add - adds n but saturates at u64::MAX
    pub fn saturating_add(self, n: u64) -> Self {
        match self.0.get().checked_add(n) {
            Some(result) => Self(NonZeroU64::new(result).unwrap_or(NonZeroU64::MAX)),
            None => Self(NonZeroU64::MAX),
        }
    }
}

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
    /// Atomically append entries to the log
    /// Returns the last sequence number assigned
    async fn append(
        &self,
        namespace: &StorageNamespace,
        entries: Arc<Vec<Bytes>>,
    ) -> StorageResult<LogIndex>;

    /// Put entries at specific indices
    /// The entries vector contains (index, data) pairs where the index specifies
    /// the exact position where each entry should be stored
    async fn put_at(
        &self,
        namespace: &StorageNamespace,
        entries: Vec<(LogIndex, Arc<Bytes>)>,
    ) -> StorageResult<()>;

    /// Get the current bounds of the log (first_index, last_index)
    async fn bounds(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Option<(LogIndex, LogIndex)>>;

    /// Remove all entries up to and including the given index
    async fn compact_before(
        &self,
        namespace: &StorageNamespace,
        index: LogIndex,
    ) -> StorageResult<()>;

    /// Read a range of entries [start, end)
    async fn read_range(
        &self,
        namespace: &StorageNamespace,
        start: LogIndex,
        end: LogIndex,
    ) -> StorageResult<Vec<(LogIndex, Bytes)>>;

    /// Remove all entries after the given index
    async fn truncate_after(
        &self,
        namespace: &StorageNamespace,
        index: LogIndex,
    ) -> StorageResult<()>;

    /// Get metadata value by key
    async fn get_metadata(
        &self,
        namespace: &StorageNamespace,
        key: &str,
    ) -> StorageResult<Option<Bytes>>;

    /// Set metadata value by key
    async fn set_metadata(
        &self,
        namespace: &StorageNamespace,
        key: &str,
        value: Bytes,
    ) -> StorageResult<()>;
}

/// Extended log storage trait that supports deletion of individual entries
/// This trait is separate from LogStorage to ensure consensus logs remain immutable
/// while allowing application streams to support deletion
#[async_trait]
pub trait LogStorageWithDelete: LogStorage {
    /// Delete or tombstone a specific entry at the given index
    /// Returns true if the entry existed and was deleted, false if it didn't exist
    async fn delete_entry(
        &self,
        namespace: &StorageNamespace,
        index: LogIndex,
    ) -> StorageResult<bool>;
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
        start: LogIndex,
        end: Option<LogIndex>,
    ) -> StorageResult<Box<dyn Stream<Item = StorageResult<(LogIndex, Bytes)>> + Send + Unpin>>;
}

/// Implement LogStorage for Arc<T> where T: LogStorage
#[async_trait]
impl<T: LogStorage> LogStorage for std::sync::Arc<T> {
    async fn append(
        &self,
        namespace: &StorageNamespace,
        entries: Arc<Vec<Bytes>>,
    ) -> StorageResult<LogIndex> {
        (**self).append(namespace, entries).await
    }

    async fn put_at(
        &self,
        namespace: &StorageNamespace,
        entries: Vec<(LogIndex, Arc<Bytes>)>,
    ) -> StorageResult<()> {
        (**self).put_at(namespace, entries).await
    }

    async fn bounds(
        &self,
        namespace: &StorageNamespace,
    ) -> StorageResult<Option<(LogIndex, LogIndex)>> {
        (**self).bounds(namespace).await
    }

    async fn compact_before(
        &self,
        namespace: &StorageNamespace,
        index: LogIndex,
    ) -> StorageResult<()> {
        (**self).compact_before(namespace, index).await
    }

    async fn read_range(
        &self,
        namespace: &StorageNamespace,
        start: LogIndex,
        end: LogIndex,
    ) -> StorageResult<Vec<(LogIndex, Bytes)>> {
        (**self).read_range(namespace, start, end).await
    }

    async fn truncate_after(
        &self,
        namespace: &StorageNamespace,
        index: LogIndex,
    ) -> StorageResult<()> {
        (**self).truncate_after(namespace, index).await
    }

    async fn get_metadata(
        &self,
        namespace: &StorageNamespace,
        key: &str,
    ) -> StorageResult<Option<Bytes>> {
        (**self).get_metadata(namespace, key).await
    }

    async fn set_metadata(
        &self,
        namespace: &StorageNamespace,
        key: &str,
        value: Bytes,
    ) -> StorageResult<()> {
        (**self).set_metadata(namespace, key, value).await
    }
}

/// Implement LogStorageWithDelete for Arc<T> where T: LogStorageWithDelete
#[async_trait]
impl<T: LogStorageWithDelete> LogStorageWithDelete for std::sync::Arc<T> {
    async fn delete_entry(
        &self,
        namespace: &StorageNamespace,
        index: LogIndex,
    ) -> StorageResult<bool> {
        (**self).delete_entry(namespace, index).await
    }
}

/// Implement LogStorageStreaming for Arc<T> where T: LogStorageStreaming
#[async_trait]
impl<T: LogStorageStreaming> LogStorageStreaming for std::sync::Arc<T> {
    async fn stream_range(
        &self,
        namespace: &StorageNamespace,
        start: LogIndex,
        end: Option<LogIndex>,
    ) -> StorageResult<Box<dyn Stream<Item = StorageResult<(LogIndex, Bytes)>> + Send + Unpin>>
    {
        (**self).stream_range(namespace, start, end).await
    }
}
