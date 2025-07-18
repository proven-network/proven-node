//! Simplified storage traits for Proven consensus
//!
//! This crate provides a minimal log storage interface without imposing
//! implementation details on storage backends.

pub mod log;

// Re-export the essential types
pub use log::{
    LogStorage, LogStorageWithDelete, StorageError, StorageKey, StorageNamespace, StorageResult,
};

// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
