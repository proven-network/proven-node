//! Stream storage traits and implementations

use async_trait::async_trait;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use proven_storage::{
    LogIndex, LogStorageWithDelete, StorageError, StorageNamespace, StorageResult,
};

use super::types::MessageData;
use crate::foundation::messages::format as message_format;
use crate::foundation::{StoredMessage, StreamName};

/// Stream storage reader interface
#[async_trait]
pub trait StreamStorageReader: Send + Sync {
    /// Read messages from the stream
    async fn read_range(&self, start: LogIndex, end: LogIndex)
    -> StorageResult<Vec<StoredMessage>>;

    /// Get the last sequence number
    async fn last_sequence(&self) -> StorageResult<Option<LogIndex>>;

    /// Get stream bounds
    async fn bounds(&self) -> StorageResult<Option<(LogIndex, LogIndex)>>;
}

/// Stream storage writer interface
#[async_trait]
pub trait StreamStorageWriter: Send + Sync {
    /// Append a message to the stream
    async fn append(
        &self,
        seq: LogIndex,
        message: MessageData,
        timestamp: u64,
    ) -> StorageResult<()>;

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
        /// BTreeMap of sequence numbers to messages
        data: Arc<RwLock<BTreeMap<LogIndex, StoredMessage>>>,
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
    async fn read_range(
        &self,
        start: LogIndex,
        end: LogIndex,
    ) -> StorageResult<Vec<StoredMessage>> {
        match &self.backend {
            StreamStorageBackend::Ephemeral { data } => {
                let data = data.read().await;
                Ok(data.range(start..end).map(|(_, msg)| msg.clone()).collect())
            }
            StreamStorageBackend::Persistent { storage, namespace } => {
                let entries = storage.read_range(namespace, start, end).await?;

                let mut results = Vec::new();
                for (seq, bytes) in entries {
                    match message_format::deserialize_entry(&bytes) {
                        Ok((message, timestamp, _sequence)) => {
                            results.push(StoredMessage {
                                sequence: seq,
                                data: message,
                                timestamp,
                            });
                        }
                        Err(e) => {
                            return Err(StorageError::InvalidValue(format!(
                                "Failed to deserialize message at sequence {seq}: {e}"
                            )));
                        }
                    }
                }
                Ok(results)
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
}

#[async_trait]
impl<L: LogStorageWithDelete> StreamStorageWriter for StreamStorageImpl<L> {
    async fn append(
        &self,
        seq: LogIndex,
        message: MessageData,
        timestamp: u64,
    ) -> StorageResult<()> {
        let stored_message = StoredMessage {
            sequence: seq,
            data: message,
            timestamp,
        };

        match &self.backend {
            StreamStorageBackend::Ephemeral { data } => {
                data.write().await.insert(seq, stored_message);
                Ok(())
            }
            StreamStorageBackend::Persistent { storage, namespace } => {
                let mut buffer = Vec::new();
                ciborium::into_writer(&stored_message, &mut buffer)
                    .map_err(|e| StorageError::InvalidValue(format!("Failed to serialize: {e}")))?;

                storage
                    .put_at(namespace, vec![(seq, Arc::new(Bytes::from(buffer)))])
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
