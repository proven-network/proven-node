//! Group storage module for Raft consensus operations
//!
//! This module contains all storage implementations specifically for Raft
//! consensus operations within local consensus groups, separate from stream data.

/// Log types for local Raft storage
pub mod log_types;

/// Raft-specific storage implementation
pub mod raft_storage;

/// Factory for creating Raft storage instances
pub mod raft_factory;

/// Tests for LogStorage implementation
#[cfg(test)]
mod test_log_storage;

// Re-export commonly used types
pub use raft_factory::{
    GroupRaftStorage, GroupRaftStorageFactory, MemoryRaftStorageFactory, RocksDBRaftStorageFactory,
    create_raft_storage_factory,
};

pub use raft_storage::{RaftSnapshotBuilder, RaftStorage};
