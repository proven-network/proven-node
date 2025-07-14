//! Stream migration system for consensus groups
//!
//! This module provides functionality for migrating streams between consensus groups
//! to balance load, handle failures, and optimize performance.

pub mod stream_migration;
pub mod types;

pub use stream_migration::{
    ActiveStreamMigration, CompressionType, MigrationCheckpoint, StorageType,
    StreamMigrationConfig, StreamMigrationCoordinator, StreamMigrationProgress,
};

pub use types::{MessageSourceType, MigrationStreamMetadata, StreamMessage};
