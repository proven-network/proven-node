//! Stream module for stream-specific consensus operations
//!
//! This module contains all stream-related functionality including storage,
//! management, and stream-specific operations.

/// Stream storage implementations
pub mod storage;

// Re-export commonly used types
pub use storage::{
    StreamManager, StreamMetadata, StreamStorageBackend, UnifiedStreamManager,
    create_stream_manager_with_backend,
};
