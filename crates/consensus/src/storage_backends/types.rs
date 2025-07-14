//! Common types for the storage adaptor layer

use bytes::Bytes;
use std::fmt::{Debug, Display};

/// Result type for storage operations
pub type StorageResult<T> = Result<T, StorageError>;

/// Errors that can occur in storage operations
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Key not found
    #[error("Key not found: {0}")]
    KeyNotFound(String),

    /// Namespace not found
    #[error("Namespace not found: {0}")]
    NamespaceNotFound(String),

    /// Invalid key format
    #[error("Invalid key format: {0}")]
    InvalidKey(String),

    /// Invalid value format
    #[error("Invalid value format: {0}")]
    InvalidValue(String),

    /// Storage backend error
    #[error("Storage backend error: {0}")]
    Backend(String),

    /// Operation not supported
    #[error("Operation not supported: {0}")]
    NotSupported(String),

    /// Batch operation failed
    #[error("Batch operation failed: {0}")]
    BatchFailed(String),

    /// Snapshot error
    #[error("Snapshot error: {0}")]
    SnapshotError(String),

    /// Encryption error
    #[error("Encryption error: {0}")]
    EncryptionError(String),

    /// WAL error
    #[error("WAL error: {0}")]
    WalError(String),
}

/// A key in the storage system
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorageKey(pub Bytes);

impl StorageKey {
    /// Create a new storage key from bytes
    pub fn new(key: impl Into<Bytes>) -> Self {
        Self(key.into())
    }

    /// Create a storage key from a string
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Self {
        Self(Bytes::copy_from_slice(s.as_bytes()))
    }

    /// Create a storage key from a u64 (big-endian encoding for ordering)
    pub fn from_u64(value: u64) -> Self {
        Self(Bytes::copy_from_slice(&value.to_be_bytes()))
    }

    /// Get the raw bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Convert to a vector
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }
}

impl Display for StorageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Try to display as UTF-8 string if possible, otherwise hex
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

impl From<Vec<u8>> for StorageKey {
    fn from(bytes: Vec<u8>) -> Self {
        Self(Bytes::from(bytes))
    }
}

impl From<&str> for StorageKey {
    fn from(s: &str) -> Self {
        Self::from_str(s)
    }
}

/// A value in the storage system
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageValue(pub Bytes);

impl StorageValue {
    /// Create a new storage value from bytes
    pub fn new(value: impl Into<Bytes>) -> Self {
        Self(value.into())
    }

    /// Get the raw bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Convert to a vector
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    /// Get the size in bytes
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the value is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A namespace for organizing data (maps to column families in RocksDB)
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct StorageNamespace(String);

impl StorageNamespace {
    /// Create a new namespace
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Get the namespace name
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for StorageNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Common namespaces used across storage implementations
pub mod namespaces {
    use super::StorageNamespace;

    /// Default namespace for general storage
    pub const DEFAULT: &str = "default";

    /// Namespace for Raft log entries
    pub const LOGS: &str = "logs";

    /// Namespace for snapshots
    pub const SNAPSHOTS: &str = "snapshots";

    /// Namespace for metadata
    pub const METADATA: &str = "metadata";

    /// Create a stream-specific namespace
    pub fn stream(stream_name: &str) -> StorageNamespace {
        StorageNamespace::new(format!("stream_{stream_name}"))
    }
}

/// Iterator over storage entries
pub trait StorageIterator: Send {
    /// Get the next key-value pair
    fn next(&mut self) -> StorageResult<Option<(StorageKey, StorageValue)>>;

    /// Seek to a specific key
    fn seek(&mut self, key: &StorageKey) -> StorageResult<()>;

    /// Check if the iterator is valid
    fn valid(&self) -> bool;
}

/// A batch of write operations
pub struct WriteBatch {
    operations: Vec<BatchOperation>,
}

/// Individual operation in a write batch
pub(crate) enum BatchOperation {
    Put {
        namespace: StorageNamespace,
        key: StorageKey,
        value: StorageValue,
    },
    Delete {
        namespace: StorageNamespace,
        key: StorageKey,
    },
}

impl WriteBatch {
    /// Create a new empty write batch
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    /// Add a put operation to the batch
    pub fn put(
        &mut self,
        namespace: StorageNamespace,
        key: StorageKey,
        value: StorageValue,
    ) -> &mut Self {
        self.operations.push(BatchOperation::Put {
            namespace,
            key,
            value,
        });
        self
    }

    /// Add a delete operation to the batch
    pub fn delete(&mut self, namespace: StorageNamespace, key: StorageKey) -> &mut Self {
        self.operations
            .push(BatchOperation::Delete { namespace, key });
        self
    }

    /// Get the number of operations in the batch
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Clear all operations from the batch
    pub fn clear(&mut self) {
        self.operations.clear();
    }

    /// Consume the batch and return the operations
    pub(crate) fn into_operations(self) -> Vec<BatchOperation> {
        self.operations
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for storage adaptors
#[derive(Clone, Debug)]
pub struct StorageConfig {
    /// Enable write-ahead logging
    pub wal_enabled: bool,

    /// Enable encryption at rest
    pub encryption_enabled: bool,

    /// Compression type
    pub compression: CompressionType,

    /// Sync writes to disk
    pub sync_writes: bool,

    /// Cache size in bytes (for backends that support caching)
    pub cache_size: Option<usize>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            wal_enabled: true,
            encryption_enabled: false,
            compression: CompressionType::None,
            sync_writes: true,
            cache_size: None,
        }
    }
}

impl From<&[u8]> for StorageValue {
    fn from(bytes: &[u8]) -> Self {
        Self(Bytes::copy_from_slice(bytes))
    }
}

impl From<Vec<u8>> for StorageValue {
    fn from(bytes: Vec<u8>) -> Self {
        Self(Bytes::from(bytes))
    }
}

impl From<Bytes> for StorageValue {
    fn from(bytes: Bytes) -> Self {
        Self(bytes)
    }
}

/// Compression types supported by storage adaptors
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompressionType {
    /// No compression
    None,
    /// LZ4 compression
    Lz4,
    /// Zstandard compression
    Zstd,
    /// Snappy compression
    Snappy,
}
