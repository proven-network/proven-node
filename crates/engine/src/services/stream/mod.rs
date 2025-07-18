//! Stream service module
//!
//! This service manages stream storage and provides stream operations.

pub mod config;
pub mod service;
pub mod storage;
pub mod types;

// Re-export the main types
pub use config::{PersistenceType, RetentionPolicy, StreamConfig};
pub use service::{StreamMetadata, StreamService, StreamServiceConfig};
pub use storage::{StreamStorageImpl, StreamStorageReader, StreamStorageWriter};
pub use types::{
    MessageData, MessageData as StreamMessage, StoredMessage, StreamName, StreamState,
};
