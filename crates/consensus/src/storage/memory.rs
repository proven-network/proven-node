//! In-memory storage implementation for consensus

use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, RaftLogReader, RaftSnapshotBuilder, SnapshotMeta, StorageError,
    StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::apply_request_to_state_machine;
use crate::snapshot::SnapshotData;
use crate::state_machine::StreamStore;
use crate::types::{MessagingResponse, TypeConfig};

/// Simple in-memory storage for consensus
#[derive(Debug, Clone)]
pub struct MemoryConsensusStorage {
    /// Current vote state
    vote: Arc<RwLock<Option<Vote<TypeConfig>>>>,
    /// Log entries
    log: Arc<RwLock<BTreeMap<u64, Entry<TypeConfig>>>>,
    /// State machine data
    state_machine: Arc<RwLock<MemoryStateMachine>>,
    /// Last purged log ID
    last_purged: Arc<RwLock<Option<LogId<TypeConfig>>>>,
    /// Stream store reference for snapshot building
    stream_store: Option<Arc<StreamStore>>,
}

/// Simple in-memory state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryStateMachine {
    /// Last applied log ID
    pub last_applied_log: Option<LogId<TypeConfig>>,
    /// State machine data (simplified)
    pub data: BTreeMap<String, String>,
    /// Stream store snapshot data
    pub stream_store_snapshot: Option<SnapshotData>,
    /// Last membership
    pub last_membership: StoredMembership<TypeConfig>,
}

/// Snapshot builder for memory storage
pub struct MemorySnapshotBuilder {
    last_applied: Option<LogId<TypeConfig>>,
    last_membership: StoredMembership<TypeConfig>,
    stream_store: Option<Arc<StreamStore>>,
}

impl MemoryConsensusStorage {
    /// Create new in-memory storage
    pub fn new() -> Self {
        Self {
            vote: Arc::new(RwLock::new(None)),
            log: Arc::new(RwLock::new(BTreeMap::new())),
            state_machine: Arc::new(RwLock::new(MemoryStateMachine {
                last_applied_log: None,
                data: BTreeMap::new(),
                stream_store_snapshot: None,
                last_membership: StoredMembership::default(),
            })),
            last_purged: Arc::new(RwLock::new(None)),
            stream_store: None,
        }
    }

    /// Create new in-memory storage with stream store reference
    pub fn new_with_stream_store(stream_store: Arc<StreamStore>) -> Self {
        let mut storage = Self::new();
        storage.set_stream_store(stream_store);
        storage
    }

    /// Set the stream store reference
    pub fn set_stream_store(&mut self, stream_store: Arc<StreamStore>) {
        // Store the stream store reference for snapshot building
        // This will be used when creating snapshot builders
        self.stream_store = Some(stream_store);
    }
}

impl Default for MemoryConsensusStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftLogReader<TypeConfig> for MemoryConsensusStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<TypeConfig>> {
        let log = self.log.read().await;
        let mut entries = Vec::new();

        for (_, entry) in log.range(range) {
            entries.push(entry.clone());
        }

        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<TypeConfig>>, StorageError<TypeConfig>> {
        Ok(self.vote.read().await.clone())
    }
}

impl RaftLogStorage<TypeConfig> for MemoryConsensusStorage {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<TypeConfig>> {
        let log = self.log.read().await;
        let last_purged_log_id = self.last_purged.read().await.clone();

        let last_log_id = log.values().last().map(|entry| entry.log_id().clone());

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<TypeConfig>) -> Result<(), StorageError<TypeConfig>> {
        *self.vote.write().await = Some(vote.clone());
        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), StorageError<TypeConfig>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut log = self.log.write().await;

        for entry in entries {
            log.insert(entry.log_id().index, entry);
        }

        callback.io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<TypeConfig>,
    ) -> Result<(), StorageError<TypeConfig>> {
        let mut log = self.log.write().await;

        // Remove all entries after log_id
        let mut to_remove = Vec::new();
        for (&index, _) in log.iter() {
            if index > log_id.index {
                to_remove.push(index);
            }
        }

        for index in to_remove {
            log.remove(&index);
        }

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<TypeConfig>) -> Result<(), StorageError<TypeConfig>> {
        let mut log = self.log.write().await;
        let mut last_purged = self.last_purged.write().await;

        // Remove all entries up to and including log_id
        let mut to_remove = Vec::new();
        for (&index, _) in log.iter() {
            if index <= log_id.index {
                to_remove.push(index);
            }
        }

        for index in to_remove {
            log.remove(&index);
        }

        *last_purged = Some(log_id);
        Ok(())
    }
}

impl RaftStateMachine<TypeConfig> for MemoryConsensusStorage {
    type SnapshotBuilder = MemorySnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<TypeConfig>>, StoredMembership<TypeConfig>), StorageError<TypeConfig>>
    {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log.clone(),
            state_machine.last_membership.clone(),
        ))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<MessagingResponse>, StorageError<TypeConfig>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();
        let mut state_machine = self.state_machine.write().await;

        for entry in entries {
            let log_id = entry.log_id().clone();
            state_machine.last_applied_log = Some(log_id.clone());

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(MessagingResponse {
                        sequence: log_id.index,
                        success: true,
                        error: None,
                    });
                }
                EntryPayload::Normal(request) => {
                    // Apply the request to the simple state machine for persistence
                    let response = apply_request_to_state_machine(
                        &mut state_machine.data,
                        &request,
                        log_id.index,
                    );

                    // Also apply to StreamStore if available
                    if let Some(stream_store) = &self.stream_store {
                        let _stream_response = stream_store
                            .apply_operation(&request.operation, log_id.index)
                            .await;
                    }

                    responses.push(response);
                }
                EntryPayload::Membership(membership) => {
                    state_machine.last_membership =
                        StoredMembership::new(Some(log_id.clone()), membership);
                    responses.push(MessagingResponse {
                        sequence: log_id.index,
                        success: true,
                        error: None,
                    });
                }
            }
        }

        Ok(responses)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Cursor<Vec<u8>>, StorageError<TypeConfig>> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<TypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), StorageError<TypeConfig>> {
        let mut state_machine = self.state_machine.write().await;
        state_machine.last_applied_log = meta.last_log_id.clone();
        state_machine.last_membership = meta.last_membership.clone();

        // If we have snapshot data, deserialize and restore it
        if !snapshot.get_ref().is_empty() {
            match SnapshotData::from_bytes(snapshot.get_ref()) {
                Ok(snapshot_data) => {
                    state_machine.stream_store_snapshot = Some(snapshot_data.clone());

                    // If we have a stream store reference, restore its state
                    if let Some(stream_store) = &self.stream_store {
                        snapshot_data.restore_to_stream_store(stream_store).await;
                    }
                }
                Err(e) => {
                    return Err(StorageError::read_snapshot(None, &e));
                }
            }
        }

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<TypeConfig>> {
        let state_machine = self.state_machine.read().await;

        // Create snapshot even if no logs have been applied yet
        // This is useful for testing and initial snapshots

        let meta = SnapshotMeta {
            last_log_id: state_machine.last_applied_log.clone(),
            last_membership: state_machine.last_membership.clone(),
            snapshot_id: format!(
                "snapshot-{}",
                state_machine
                    .last_applied_log
                    .as_ref()
                    .map_or(0, |id| id.index)
            ),
        };

        // Create snapshot data from current StreamStore if available
        let snapshot_bytes = if let Some(stream_store) = &self.stream_store {
            let snapshot_data = SnapshotData::from_stream_store(stream_store).await;
            snapshot_data.to_bytes().unwrap_or_else(|_| Vec::new())
        } else if let Some(snapshot_data) = &state_machine.stream_store_snapshot {
            snapshot_data.to_bytes().unwrap_or_else(|_| Vec::new())
        } else {
            Vec::new()
        };

        Ok(Some(Snapshot {
            meta,
            snapshot: Cursor::new(snapshot_bytes),
        }))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let state_machine = self.state_machine.read().await;
        MemorySnapshotBuilder {
            last_applied: state_machine.last_applied_log.clone(),
            last_membership: state_machine.last_membership.clone(),
            stream_store: self.stream_store.clone(),
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for MemorySnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<TypeConfig>> {
        let meta = SnapshotMeta {
            last_log_id: self.last_applied.clone(),
            last_membership: self.last_membership.clone(),
            snapshot_id: format!(
                "snapshot-{}",
                self.last_applied.as_ref().map_or(0, |id| id.index)
            ),
        };

        // Create snapshot data from StreamStore if available
        let snapshot_bytes = if let Some(stream_store) = &self.stream_store {
            let snapshot_data = SnapshotData::from_stream_store(stream_store).await;
            snapshot_data.to_bytes().unwrap_or_else(|_| Vec::new())
        } else {
            Vec::new()
        };

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(snapshot_bytes),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_machine::StreamStore;
    use crate::types::MessagingOperation;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_memory_storage_snapshot_with_stream_store() {
        let stream_store = Arc::new(StreamStore::new());
        let mut storage = MemoryConsensusStorage::new_with_stream_store(stream_store.clone());

        // Add some data to the stream store
        let data = Bytes::from("test message");
        let response = stream_store
            .apply_operation(
                &MessagingOperation::PublishToStream {
                    stream: "test-stream".to_string(),
                    data: data.clone(),
                },
                1,
            )
            .await;
        assert!(response.success);

        // Create a snapshot
        let snapshot = storage.get_current_snapshot().await.unwrap();
        assert!(snapshot.is_some());

        let snapshot = snapshot.unwrap();
        assert!(!snapshot.snapshot.get_ref().is_empty());

        // Install the snapshot on a new storage
        let new_stream_store = Arc::new(StreamStore::new());
        let mut new_storage =
            MemoryConsensusStorage::new_with_stream_store(new_stream_store.clone());

        new_storage
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();

        // Verify the data was restored
        let restored_message = new_stream_store.get_message("test-stream", 1).await;
        assert_eq!(restored_message, Some(data));
    }

    #[tokio::test]
    async fn test_memory_storage_snapshot_builder() {
        let stream_store = Arc::new(StreamStore::new());
        let mut storage = MemoryConsensusStorage::new_with_stream_store(stream_store.clone());

        // Add some data
        let data = Bytes::from("builder test");
        let response = stream_store
            .apply_operation(
                &MessagingOperation::PublishToStream {
                    stream: "builder-stream".to_string(),
                    data: data.clone(),
                },
                1,
            )
            .await;
        assert!(response.success);

        // Get snapshot builder and build snapshot (without setting applied state for simplicity)
        let mut builder = storage.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();

        assert!(!snapshot.snapshot.get_ref().is_empty());
        assert_eq!(snapshot.meta.last_log_id, None);
    }
}
