//! Stream service module
//!
//! This service manages stream storage and provides stream operations.

pub mod config;
pub mod events;
pub mod service;
pub mod storage;
pub mod subscribers;
pub mod types;

// Re-export the main types
pub use config::{PersistenceType, RetentionPolicy, StreamConfig};
pub use events::StreamEvent;
pub use service::{StreamMetadata, StreamService, StreamServiceConfig};
pub use storage::{StreamStorageImpl, StreamStorageReader, StreamStorageWriter};
pub use types::{
    MessageData, MessageData as StreamMessage, StoredMessage, StreamName, StreamState,
    deserialize_stored_message, serialize_stored_message,
};
