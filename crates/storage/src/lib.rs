//! Simplified storage traits for Proven consensus
//!
//! This crate provides a minimal log storage interface without imposing
//! implementation details on storage backends.

pub mod adaptor;
pub mod log;
pub mod manager;

// Re-export the essential types
pub use adaptor::StorageAdaptor;
pub use log::{
    LogStorage, LogStorageStreaming, LogStorageWithDelete, StorageError, StorageKey,
    StorageNamespace, StorageResult,
};
pub use manager::{ConsensusStorage, StorageManager, StreamStorage};
