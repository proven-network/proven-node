//! Storage adaptor trait for implementing storage backends
//!
//! This trait defines the interface that storage implementations must provide
//! to be used with the StorageManager.

use async_trait::async_trait;
use std::fmt::Debug;

use crate::{LogStorage, LogStorageStreaming, LogStorageWithDelete, StorageResult};

/// Abstract interface for storage backends
///
/// This trait is automatically implemented for types that implement both
/// LogStorage and LogStorageWithDelete. It provides additional methods
/// specific to storage adaptors.
#[async_trait]
pub trait StorageAdaptor:
    LogStorage + LogStorageStreaming + LogStorageWithDelete + Debug + Send + Sync + Clone + 'static
{
    /// Gracefully shutdown the storage, ensuring all data is persisted
    /// Default implementation does nothing for backward compatibility
    async fn shutdown(&self) -> StorageResult<()> {
        Ok(())
    }

    /// Delete all data in the storage (optional operation)
    /// This is useful for testing or administrative purposes
    async fn delete_all(&self) -> StorageResult<()> {
        Err(crate::StorageError::NotSupported(
            "delete_all not implemented for this storage backend".to_string(),
        ))
    }

    /// Get storage statistics (optional operation)
    /// Returns implementation-specific statistics about the storage
    async fn stats(&self) -> StorageResult<String> {
        Ok("No statistics available".to_string())
    }
}
