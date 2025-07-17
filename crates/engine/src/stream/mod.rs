//! Stream subsystem
//!
//! This module provides the core stream functionality including:
//! - Stream configuration and metadata
//! - Stream storage abstractions
//! - Stream state management
//! - Stream reader interfaces

pub mod config;
pub mod service;
pub mod storage;
pub mod types;

pub use config::{PersistenceType, StreamConfig};
pub use service::{StreamService, StreamServiceConfig};
pub use storage::{StreamStorage, StreamStorageImpl, StreamStorageReader, StreamStorageWriter};
pub use types::{MessageData, StoredMessage, StreamMetadata, StreamName, StreamState, StreamStats};
