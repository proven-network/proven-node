//! Stream storage module for stream data operations
//!
//! This module contains all storage implementations for stream data,
//! including per-stream storage, stream managers, and stream-specific configurations.

/// Log types specific to stream storage
pub mod log_types;

/// Storage traits for stream-oriented operations
pub mod traits;

/// Stream manager for managing per-stream storage instances
pub mod stream_manager;

/// Per-stream storage factory
pub mod per_stream_factory;

/// Migration iterators for efficient streaming
pub mod migration_iterator;

/// Unified stream storage type
pub mod unified;

/// Tests for LogStorage with stream storage
#[cfg(test)]
mod test_stream_log_storage;

// Re-export commonly used types
pub use per_stream_factory::{
    PerStreamStorageFactory, StreamPersistenceMode, StreamStorageBackend,
};
pub use stream_manager::{
    StreamManager, UnifiedStreamManager, create_stream_manager, create_stream_manager_with_backend,
};
pub use traits::{
    CompressionType, RetentionPolicy, StorageType, StreamConfig, StreamExport, StreamMetadata,
    StreamMetrics, StreamOptions,
};
