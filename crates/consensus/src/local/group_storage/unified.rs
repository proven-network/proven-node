//! Unified local consensus storage implementation
//!
//! This module provides a unified storage implementation that combines
//! Raft log storage and state machine functionality, similar to GlobalStorage.

use super::log_types::{LocalEntryType, LocalLogMetadata, StreamOperationType};
use crate::{
    allocation::ConsensusGroupId,
    error::{ConsensusResult, Error},
    local::{
        LocalResponse, LocalTypeConfig, StorageBackedLocalState,
        state_command::{LocalCommandFactory, LocalCommandProcessor},
    },
    node_id::NodeId,
    storage::{StorageEngine, StorageValue, log::LogStorage},
};
use openraft::{
    Entry, LogId, SnapshotMeta, StorageError, StoredMembership, Vote,
    storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftStateMachine, Snapshot},
};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, io::Cursor, ops::RangeBounds, sync::Arc};
use tokio::sync::RwLock;
use tracing::{debug, error};

/// Namespaces for Raft storage
mod namespaces {
    use crate::storage::StorageNamespace;

    pub fn logs() -> StorageNamespace {
        StorageNamespace::new("raft_logs")
    }

    pub fn state() -> StorageNamespace {
        StorageNamespace::new("raft_state")
    }

    pub fn snapshots() -> StorageNamespace {
        StorageNamespace::new("raft_snapshots")
    }
}

/// Keys for Raft storage
mod keys {
    use crate::storage::StorageKey;

    pub fn vote() -> StorageKey {
        StorageKey::from("vote")
    }

    pub fn membership() -> StorageKey {
        StorageKey::from("membership")
    }

    pub fn last_applied() -> StorageKey {
        StorageKey::from("last_applied")
    }

    #[allow(dead_code)]
    pub fn log_entry(index: u64) -> StorageKey {
        StorageKey::from(format!("log:{:016x}", index).as_str())
    }

    pub fn snapshot_meta() -> StorageKey {
        StorageKey::from("snapshot_meta")
    }

    pub fn snapshot_data() -> StorageKey {
        StorageKey::from("snapshot_data")
    }
}

/// Unified local storage that combines Raft storage with state machine
#[derive(Clone)]
pub struct LocalStorage<S: StorageEngine + LogStorage<LocalLogMetadata>> {
    /// The underlying storage engine
    storage: Arc<S>,
    /// Group ID for this storage instance
    group_id: ConsensusGroupId,
    /// Local state machine reference
    local_state: Arc<RwLock<StorageBackedLocalState>>,
}

impl<S: StorageEngine + LogStorage<LocalLogMetadata>> LocalStorage<S> {
    /// Create a new unified local storage instance
    pub async fn new(
        storage: Arc<S>,
        group_id: ConsensusGroupId,
        local_state: Arc<RwLock<StorageBackedLocalState>>,
    ) -> ConsensusResult<Self> {
        // Create namespaces
        storage
            .create_namespace(&namespaces::logs())
            .await
            .map_err(|e| Error::storage(e.to_string()))?;
        storage
            .create_namespace(&namespaces::state())
            .await
            .map_err(|e| Error::storage(e.to_string()))?;
        storage
            .create_namespace(&namespaces::snapshots())
            .await
            .map_err(|e| Error::storage(e.to_string()))?;

        Ok(Self {
            storage,
            group_id,
            local_state,
        })
    }
}

/// Stored vote structure
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredVote {
    term: u64,
    node_id: NodeId,
    committed: bool,
}

impl From<&Vote<LocalTypeConfig>> for StoredVote {
    fn from(vote: &Vote<LocalTypeConfig>) -> Self {
        Self {
            term: vote.leader_id.term,
            node_id: vote.leader_id.node_id.clone(),
            committed: vote.committed,
        }
    }
}

impl StoredVote {
    fn to_vote(&self) -> Vote<LocalTypeConfig> {
        Vote {
            leader_id: openraft::vote::leader_id_adv::LeaderId {
                term: self.term,
                node_id: self.node_id.clone(),
            },
            committed: self.committed,
        }
    }
}

impl<S: StorageEngine + LogStorage<LocalLogMetadata> + Clone> Debug for LocalStorage<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalStorage")
            .field("storage", &"<storage_engine>")
            .field("group_id", &self.group_id)
            .finish()
    }
}

impl<S: StorageEngine + LogStorage<LocalLogMetadata> + Clone> RaftLogReader<LocalTypeConfig>
    for LocalStorage<S>
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<LocalTypeConfig>>, StorageError<LocalTypeConfig>> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(&n) => n,
            std::ops::Bound::Excluded(&n) => n + 1,
            std::ops::Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(&n) => n + 1,
            std::ops::Bound::Excluded(&n) => n,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        // Use direct storage access to read entries
        let mut entries = Vec::new();

        for index in start..end {
            match self.storage.get_entry(&namespaces::logs(), index).await {
                Ok(Some(log_entry)) => {
                    // Extract the OpenRaft Entry from our LogEntry wrapper
                    let entry: Entry<LocalTypeConfig> =
                        ciborium::from_reader(log_entry.data.as_ref())
                            .map_err(|e| StorageError::read_logs(&e))?;
                    entries.push(entry);
                }
                Ok(None) => break, // No more entries
                Err(e) => return Err(StorageError::read_logs(&e)),
            }
        }

        Ok(entries)
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<LocalTypeConfig>>, StorageError<LocalTypeConfig>> {
        match self.storage.get(&namespaces::state(), &keys::vote()).await {
            Ok(Some(value)) => {
                let stored: StoredVote = ciborium::from_reader(value.as_bytes())
                    .map_err(|e| StorageError::read_vote(&e))?;
                Ok(Some(stored.to_vote()))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::read_vote(&e)),
        }
    }
}

impl<S: StorageEngine + LogStorage<LocalLogMetadata> + Clone> RaftLogStorage<LocalTypeConfig>
    for LocalStorage<S>
{
    type LogReader = Self;

    async fn save_vote(
        &mut self,
        vote: &Vote<LocalTypeConfig>,
    ) -> Result<(), StorageError<LocalTypeConfig>> {
        let stored = StoredVote::from(vote);
        let mut buffer = Vec::new();
        ciborium::into_writer(&stored, &mut buffer).map_err(|e| StorageError::write_vote(&e))?;

        self.storage
            .put(
                &namespaces::state(),
                keys::vote(),
                StorageValue::new(buffer),
            )
            .await
            .map_err(|e| StorageError::write_vote(&e))?;

        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<LocalTypeConfig>,
    ) -> Result<(), StorageError<LocalTypeConfig>>
    where
        I: IntoIterator<Item = Entry<LocalTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        // Create a batch for efficient writing
        let mut batch = self.storage.create_batch().await;

        for entry in entries {
            // Create metadata for the log entry
            let metadata = LocalLogMetadata {
                term: entry.log_id.leader_id.term,
                leader_node_id: entry.log_id.leader_id.node_id.clone(),
                entry_type: match &entry.payload {
                    openraft::EntryPayload::Normal(_) => LocalEntryType::StreamOperation {
                        stream_id: String::new(), // TODO: Extract from request
                        operation: StreamOperationType::Publish,
                    },
                    openraft::EntryPayload::Membership(_) => LocalEntryType::MembershipChange,
                    openraft::EntryPayload::Blank => LocalEntryType::Empty,
                },
                group_id: self.group_id,
            };

            // Serialize the entry for storage
            let mut buffer = Vec::new();
            if let Err(e) = ciborium::into_writer(&entry, &mut buffer) {
                callback.io_completed(Err(std::io::Error::other(format!(
                    "Failed to serialize entry: {}",
                    e
                ))));
                return Err(StorageError::write_logs(&e));
            }

            // Create log entry for LogStorage
            let log_entry = crate::storage::log::LogEntry {
                index: entry.log_id.index,
                timestamp: chrono::Utc::now().timestamp() as u64,
                data: bytes::Bytes::from(buffer),
                metadata,
            };

            batch.append(namespaces::logs(), log_entry);
        }

        // Apply the batch
        if let Err(e) = self.storage.apply_batch(batch).await {
            callback.io_completed(Err(std::io::Error::other(format!("Storage error: {}", e))));
            return Err(StorageError::write_logs(&e));
        }

        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<LocalTypeConfig>,
    ) -> Result<(), StorageError<LocalTypeConfig>> {
        // Use LogStorage's truncate method
        self.storage
            .truncate(&namespaces::logs(), log_id.index)
            .await
            .map_err(|e| StorageError::write_logs(&e))?;

        Ok(())
    }

    async fn purge(
        &mut self,
        log_id: LogId<LocalTypeConfig>,
    ) -> Result<(), StorageError<LocalTypeConfig>> {
        // Use LogStorage's purge method
        self.storage
            .purge(&namespaces::logs(), log_id.index)
            .await
            .map_err(|e| StorageError::write_logs(&e))?;

        Ok(())
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<LocalTypeConfig>, StorageError<LocalTypeConfig>> {
        // Use LogStorage's get_log_state method
        let state = self
            .storage
            .get_log_state(&namespaces::logs())
            .await
            .map_err(|e| StorageError::read_logs(&e))?;

        // Convert to OpenRaft LogState
        let last_log_id = if let Some(_state) = state {
            // Get the last entry to extract the full LogId
            if let Some(last_entry) = self
                .storage
                .get_last_entry(&namespaces::logs())
                .await
                .map_err(|e| StorageError::read_logs(&e))?
            {
                // Deserialize to get the OpenRaft Entry
                let entry: Entry<LocalTypeConfig> = ciborium::from_reader(last_entry.data.as_ref())
                    .map_err(|e| StorageError::read_logs(&e))?;
                Some(entry.log_id)
            } else {
                None
            }
        } else {
            None
        };

        Ok(LogState {
            last_purged_log_id: None, // TODO: Track purged log ID
            last_log_id,
        })
    }
}

/// Snapshot builder for local storage
pub struct LocalSnapshotBuilder<S: StorageEngine + LogStorage<LocalLogMetadata>> {
    #[allow(dead_code)]
    storage: Arc<S>,
}

impl<S: StorageEngine + LogStorage<LocalLogMetadata> + Clone>
    openraft::RaftSnapshotBuilder<LocalTypeConfig> for LocalSnapshotBuilder<S>
{
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<LocalTypeConfig>, StorageError<LocalTypeConfig>> {
        // For now, return an empty snapshot
        // In a real implementation, this would serialize the state machine state
        let data = Vec::new();
        let snapshot = Snapshot {
            meta: SnapshotMeta {
                last_log_id: None,
                last_membership: StoredMembership::default(),
                snapshot_id: "empty".to_string(),
            },
            snapshot: Cursor::new(data),
        };

        Ok(snapshot)
    }
}

impl<S: StorageEngine + LogStorage<LocalLogMetadata> + Clone> RaftStateMachine<LocalTypeConfig>
    for LocalStorage<S>
{
    type SnapshotBuilder = LocalSnapshotBuilder<S>;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<LocalTypeConfig>>,
            StoredMembership<LocalTypeConfig>,
        ),
        StorageError<LocalTypeConfig>,
    > {
        // Get last applied log ID
        let last_applied = match self
            .storage
            .get(&namespaces::state(), &keys::last_applied())
            .await
        {
            Ok(Some(value)) => {
                let log_id: LogId<LocalTypeConfig> = ciborium::from_reader(value.as_bytes())
                    .map_err(|e| StorageError::read_state_machine(&e))?;
                Some(log_id)
            }
            Ok(None) => None,
            Err(e) => return Err(StorageError::read_state_machine(&e)),
        };

        // Get membership
        let membership = match self
            .storage
            .get(&namespaces::state(), &keys::membership())
            .await
        {
            Ok(Some(value)) => ciborium::from_reader(value.as_bytes())
                .map_err(|e| StorageError::read_state_machine(&e))?,
            Ok(None) => StoredMembership::default(),
            Err(e) => return Err(StorageError::read_state_machine(&e)),
        };

        Ok((last_applied, membership))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<LocalResponse>, StorageError<LocalTypeConfig>>
    where
        I: IntoIterator<Item = Entry<LocalTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();
        let mut last_applied = None;

        for entry in entries {
            // Process the entry payload
            match entry.payload {
                openraft::EntryPayload::Normal(ref request) => {
                    debug!(
                        "Processing local request for group {:?}: {:?}",
                        self.group_id,
                        request.operation.operation_name()
                    );

                    // Create command from operation
                    let command = LocalCommandFactory::from_operation(&request.operation);

                    // Get write lock on state
                    let mut state = self.local_state.write().await;

                    // Process command through command processor
                    let response =
                        match LocalCommandProcessor::process(command.as_ref(), &mut state).await {
                            Ok(response) => {
                                debug!(
                                    "Successfully processed operation {} for group {:?}",
                                    request.operation.operation_name(),
                                    self.group_id
                                );
                                response
                            }
                            Err(e) => {
                                error!(
                                    "Failed to process operation {} for group {:?}: {}",
                                    request.operation.operation_name(),
                                    self.group_id,
                                    e
                                );
                                LocalResponse {
                                    success: false,
                                    sequence: Some(entry.log_id.index),
                                    error: Some(e.to_string()),
                                    checkpoint_data: None,
                                }
                            }
                        };

                    responses.push(response);
                }
                openraft::EntryPayload::Membership(ref membership) => {
                    // Process membership changes
                    let mut buffer = Vec::new();
                    ciborium::into_writer(membership, &mut buffer)
                        .map_err(|e| StorageError::write_state_machine(&e))?;

                    self.storage
                        .put(
                            &namespaces::state(),
                            keys::membership(),
                            StorageValue::new(buffer),
                        )
                        .await
                        .map_err(|e| StorageError::write_state_machine(&e))?;
                }
                openraft::EntryPayload::Blank => {
                    // No operation - this is used for heartbeats
                }
            }

            last_applied = Some(entry.log_id);
        }

        // Update last applied
        if let Some(log_id) = last_applied {
            let mut buffer = Vec::new();
            ciborium::into_writer(&log_id, &mut buffer)
                .map_err(|e| StorageError::write_state_machine(&e))?;

            self.storage
                .put(
                    &namespaces::state(),
                    keys::last_applied(),
                    StorageValue::new(buffer),
                )
                .await
                .map_err(|e| StorageError::write_state_machine(&e))?;
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        LocalSnapshotBuilder {
            storage: self.storage.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Cursor<Vec<u8>>, StorageError<LocalTypeConfig>> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<LocalTypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), StorageError<LocalTypeConfig>> {
        // Store snapshot metadata
        let mut buffer = Vec::new();
        ciborium::into_writer(meta, &mut buffer)
            .map_err(|e| StorageError::write_state_machine(&e))?;

        self.storage
            .put(
                &namespaces::snapshots(),
                keys::snapshot_meta(),
                StorageValue::new(buffer),
            )
            .await
            .map_err(|e| StorageError::write_state_machine(&e))?;

        // Store snapshot data
        let data = snapshot.into_inner();
        self.storage
            .put(
                &namespaces::snapshots(),
                keys::snapshot_data(),
                StorageValue::new(data),
            )
            .await
            .map_err(|e| StorageError::write_state_machine(&e))?;

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<LocalTypeConfig>>, StorageError<LocalTypeConfig>> {
        // Get snapshot metadata
        let meta = match self
            .storage
            .get(&namespaces::snapshots(), &keys::snapshot_meta())
            .await
        {
            Ok(Some(value)) => {
                let meta: SnapshotMeta<LocalTypeConfig> =
                    ciborium::from_reader(value.as_bytes())
                        .map_err(|e| StorageError::read_snapshot(None, &e))?;
                meta
            }
            Ok(None) => return Ok(None),
            Err(e) => return Err(StorageError::read_snapshot(None, &e)),
        };

        // Get snapshot data
        let data = match self
            .storage
            .get(&namespaces::snapshots(), &keys::snapshot_data())
            .await
        {
            Ok(Some(value)) => value.as_bytes().to_vec(),
            Ok(None) => return Ok(None),
            Err(e) => return Err(StorageError::read_snapshot(None, &e)),
        };

        Ok(Some(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        }))
    }
}
