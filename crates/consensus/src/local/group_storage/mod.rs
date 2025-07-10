//! Group storage module for Raft consensus operations
//!
//! This module contains all storage implementations specifically for Raft
//! consensus operations within local consensus groups, separate from stream data.

/// Log types for local Raft storage
pub mod log_types;

/// Unified local storage implementation
pub mod unified;

/// Factory for creating unified local storage instances
pub mod local_factory;

/// Tests for LogStorage implementation
#[cfg(test)]
mod test_log_storage;

// Re-export commonly used types
pub use local_factory::{
    LocalStorageFactory, UnifiedLocalStorage, UnifiedLocalStorageFactory,
    create_local_storage_factory,
};
pub use unified::LocalStorage;
