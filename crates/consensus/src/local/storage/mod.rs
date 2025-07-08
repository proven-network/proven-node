//! Storage implementations for local consensus groups

/// Storage traits and types for stream-oriented operations
pub mod traits;

/// Storage factory for creating isolated storage instances
pub mod factory;

/// Memory-based storage implementation
pub mod memory;

/// RocksDB-based storage implementation
pub mod rocksdb;

// Re-export commonly used types
pub use memory::{LocalMemoryStorage, LocalSnapshot, LocalSnapshotBuilder};
pub use rocksdb::{LocalRocksDBStorage, RocksDBConfig, RocksDBSnapshot, RocksDBStorageFactory};
pub use traits::{
    CheckpointFormat, CompressionType, MessageData, PauseState, StreamConfig, StreamExport,
    StreamMetadata, StreamMetrics, StreamOptions, StreamStorage,
};
