//! Stream storage traits and implementations

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use proven_storage::{
    LogIndex, LogStorageWithDelete, StorageError, StorageNamespace, StorageResult,
};

use crate::foundation::messages::{deserialize_entry, serialize_entry};
use crate::foundation::{Message, StreamName};

/// Stream storage reader interface
#[async_trait]
pub trait StreamStorageReader: Send + Sync {
    /// Read messages from the stream
    async fn read_range(&self, start: LogIndex, end: LogIndex) -> StorageResult<Vec<Message>>;

    /// Get the last sequence number
    async fn last_sequence(&self) -> StorageResult<Option<LogIndex>>;

    /// Get stream bounds
    async fn bounds(&self) -> StorageResult<Option<(LogIndex, LogIndex)>>;

    /// Read messages with metadata from the stream
    async fn read_range_with_metadata(
        &self,
        start: LogIndex,
        end: LogIndex,
    ) -> StorageResult<Vec<(Message, u64, u64)>>;
}

/// Stream storage writer interface
#[async_trait]
pub trait StreamStorageWriter: Send + Sync {
    /// Append a message to the stream
    async fn append(&self, seq: LogIndex, message: Message, timestamp: u64) -> StorageResult<()>;

    /// Compact messages before a sequence number
    async fn compact_before(&self, seq: LogIndex) -> StorageResult<()>;
}

/// Combined stream storage interface
#[async_trait]
pub trait StreamStorage: StreamStorageReader + StreamStorageWriter + Send + Sync {}

/// Storage backend for a single stream
#[derive(Clone)]
pub enum StreamStorageBackend<L: LogStorageWithDelete> {
    /// Ephemeral storage in memory
    Ephemeral {
        /// BTreeMap of sequence numbers to serialized message bytes
        data: Arc<RwLock<BTreeMap<LogIndex, Bytes>>>,
    },
    /// Persistent storage using LogStorage
    Persistent {
        /// LogStorage instance
        storage: L,
        /// Storage namespace
        namespace: StorageNamespace,
    },
}

/// Stream storage implementation
#[derive(Clone)]
pub struct StreamStorageImpl<L: LogStorageWithDelete> {
    /// Stream name
    stream_name: StreamName,
    /// Storage backend
    backend: StreamStorageBackend<L>,
}

impl<L: LogStorageWithDelete> StreamStorageImpl<L> {
    /// Create ephemeral stream storage
    pub fn ephemeral(stream_name: StreamName) -> Self {
        Self {
            stream_name,
            backend: StreamStorageBackend::Ephemeral {
                data: Arc::new(RwLock::new(BTreeMap::new())),
            },
        }
    }

    /// Create persistent stream storage
    pub fn persistent(stream_name: StreamName, storage: L, namespace: StorageNamespace) -> Self {
        Self {
            stream_name,
            backend: StreamStorageBackend::Persistent {
                storage: storage.clone(),
                namespace,
            },
        }
    }
}

#[async_trait]
impl<L: LogStorageWithDelete> StreamStorageReader for StreamStorageImpl<L> {
    async fn read_range(&self, start: LogIndex, end: LogIndex) -> StorageResult<Vec<Message>> {
        match &self.backend {
            StreamStorageBackend::Ephemeral { data } => {
                let data = data.read().await;
                let mut messages = Vec::new();
                for (_, bytes) in data.range(start..end) {
                    match deserialize_entry(bytes) {
                        Ok((message, _, _)) => messages.push(message),
                        Err(e) => {
                            return Err(StorageError::InvalidValue(format!(
                                "Failed to deserialize message: {e}"
                            )));
                        }
                    }
                }
                Ok(messages)
            }
            StreamStorageBackend::Persistent { storage, namespace } => {
                let entries = storage.read_range(namespace, start, end).await?;
                let mut messages = Vec::new();
                for (_, bytes) in entries {
                    match deserialize_entry(&bytes) {
                        Ok((message, _, _)) => messages.push(message),
                        Err(e) => {
                            return Err(StorageError::InvalidValue(format!(
                                "Failed to deserialize message: {e}"
                            )));
                        }
                    }
                }
                Ok(messages)
            }
        }
    }

    async fn last_sequence(&self) -> StorageResult<Option<LogIndex>> {
        match &self.backend {
            StreamStorageBackend::Ephemeral { data } => {
                let data = data.read().await;
                Ok(data.keys().last().copied())
            }
            StreamStorageBackend::Persistent { storage, namespace } => {
                let bounds = storage.bounds(namespace).await?;
                Ok(bounds.map(|(_, last)| last))
            }
        }
    }

    async fn bounds(&self) -> StorageResult<Option<(LogIndex, LogIndex)>> {
        match &self.backend {
            StreamStorageBackend::Ephemeral { data } => {
                let data = data.read().await;
                match (data.keys().next(), data.keys().last()) {
                    (Some(&first), Some(&last)) => Ok(Some((first, last))),
                    _ => Ok(None),
                }
            }
            StreamStorageBackend::Persistent { storage, namespace } => {
                storage.bounds(namespace).await
            }
        }
    }

    async fn read_range_with_metadata(
        &self,
        start: LogIndex,
        end: LogIndex,
    ) -> StorageResult<Vec<(Message, u64, u64)>> {
        match &self.backend {
            StreamStorageBackend::Ephemeral { data } => {
                let data = data.read().await;
                let mut results = Vec::new();
                for (_seq, bytes) in data.range(start..end) {
                    match deserialize_entry(bytes) {
                        Ok((message, timestamp, sequence)) => {
                            results.push((message, timestamp, sequence));
                        }
                        Err(e) => {
                            return Err(StorageError::InvalidValue(format!(
                                "Failed to deserialize message: {e}"
                            )));
                        }
                    }
                }
                Ok(results)
            }
            StreamStorageBackend::Persistent { storage, namespace } => {
                let entries = storage.read_range(namespace, start, end).await?;
                let mut results = Vec::new();
                for (_, bytes) in entries {
                    match deserialize_entry(&bytes) {
                        Ok((message, timestamp, sequence)) => {
                            results.push((message, timestamp, sequence));
                        }
                        Err(e) => {
                            return Err(StorageError::InvalidValue(format!(
                                "Failed to deserialize message: {e}"
                            )));
                        }
                    }
                }
                Ok(results)
            }
        }
    }
}

#[async_trait]
impl<L: LogStorageWithDelete> StreamStorageWriter for StreamStorageImpl<L> {
    async fn append(&self, seq: LogIndex, message: Message, timestamp: u64) -> StorageResult<()> {
        // Serialize the message with timestamp and sequence
        let serialized = serialize_entry(&message, timestamp, seq.get())
            .map_err(|e| StorageError::InvalidValue(format!("Failed to serialize: {e}")))?;

        match &self.backend {
            StreamStorageBackend::Ephemeral { data } => {
                data.write().await.insert(seq, serialized);
                Ok(())
            }
            StreamStorageBackend::Persistent { storage, namespace } => {
                storage
                    .put_at(namespace, vec![(seq, Arc::new(serialized))])
                    .await
            }
        }
    }

    async fn compact_before(&self, seq: LogIndex) -> StorageResult<()> {
        match &self.backend {
            StreamStorageBackend::Ephemeral { data } => {
                let mut data = data.write().await;
                data.retain(|&k, _| k >= seq);
                Ok(())
            }
            StreamStorageBackend::Persistent { storage, namespace } => {
                storage.compact_before(namespace, seq).await
            }
        }
    }
}

impl<L: LogStorageWithDelete> StreamStorage for StreamStorageImpl<L> {}
