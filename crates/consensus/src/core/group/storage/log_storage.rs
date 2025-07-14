//! Log storage implementation for group consensus
//!
//! This module provides a log storage implementation that only handles
//! Raft log persistence, following the single responsibility principle.

use crate::{
    ConsensusGroupId,
    core::group::GroupConsensusTypeConfig,
    error::{ConsensusResult, Error},
    storage_backends::{
        StorageEngine, StorageValue,
        log::{LogEntry as StorageLogEntry, LogStorage},
        traits::{Priority, StorageHints},
    },
};
use openraft::{
    Entry, EntryPayload, LogId, StorageError, Vote,
    storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage},
};
use proven_topology::NodeId;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Debug, ops::RangeBounds, sync::Arc};
use tokio::sync::RwLock;
use tracing::debug;

use super::log_types::{LocalEntryType, LocalLogMetadata, StreamOperationType};

/// Namespaces for storage
mod namespaces {
    use crate::storage_backends::StorageNamespace;

    pub fn logs() -> StorageNamespace {
        StorageNamespace::new("raft_logs")
    }

    pub fn state() -> StorageNamespace {
        StorageNamespace::new("raft_state")
    }
}

/// Keys for storage
mod keys {
    use crate::storage_backends::StorageKey;

    pub fn vote() -> StorageKey {
        StorageKey::from("vote")
    }

    pub fn last_purged() -> StorageKey {
        StorageKey::from("last_purged")
    }
}

/// Stored vote structure
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredVote {
    term: u64,
    node_id: NodeId,
    committed: bool,
}

impl From<&Vote<GroupConsensusTypeConfig>> for StoredVote {
    fn from(vote: &Vote<GroupConsensusTypeConfig>) -> Self {
        Self {
            term: vote.leader_id.term,
            node_id: vote.leader_id.node_id.clone(),
            committed: vote.committed,
        }
    }
}

impl StoredVote {
    fn to_vote(&self) -> Vote<GroupConsensusTypeConfig> {
        Vote {
            leader_id: openraft::vote::leader_id_adv::LeaderId {
                term: self.term,
                node_id: self.node_id.clone(),
            },
            committed: self.committed,
        }
    }
}

/// Group log storage that only handles log persistence
#[derive(Clone)]
pub struct GroupLogStorage<S>
where
    S: StorageEngine + LogStorage<LocalLogMetadata>,
{
    /// The underlying storage engine
    storage: Arc<S>,
    /// Group ID for this storage instance
    group_id: ConsensusGroupId,
    /// Cache for recent log entries
    log_cache: Arc<RwLock<BTreeMap<u64, Entry<GroupConsensusTypeConfig>>>>,
}

impl<S> GroupLogStorage<S>
where
    S: StorageEngine + LogStorage<LocalLogMetadata>,
{
    /// Create a new group log storage instance
    pub async fn new(storage: Arc<S>, group_id: ConsensusGroupId) -> ConsensusResult<Self> {
        // Create namespaces
        storage
            .create_namespace(&namespaces::logs())
            .await
            .map_err(|e| Error::storage(e.to_string()))?;
        storage
            .create_namespace(&namespaces::state())
            .await
            .map_err(|e| Error::storage(e.to_string()))?;

        // Initialize the storage engine
        storage
            .initialize()
            .await
            .map_err(|e| Error::storage(format!("Failed to initialize storage: {e}")))?;

        debug!(
            group_id = ?group_id,
            "Group log storage initialized successfully"
        );

        Ok(Self {
            storage,
            group_id,
            log_cache: Arc::new(RwLock::new(BTreeMap::new())),
        })
    }

    /// Convert OpenRaft log entry to storage log entry
    fn to_storage_log_entry(
        &self,
        entry: &Entry<GroupConsensusTypeConfig>,
    ) -> StorageLogEntry<LocalLogMetadata> {
        let metadata = LocalLogMetadata {
            term: entry.log_id.leader_id.term,
            leader_node_id: entry.log_id.leader_id.node_id.clone(),
            entry_type: match &entry.payload {
                EntryPayload::Normal(_) => LocalEntryType::StreamOperation {
                    stream_id: String::new(), // TODO: Extract from request
                    operation: StreamOperationType::Publish,
                },
                EntryPayload::Membership(_) => LocalEntryType::MembershipChange,
                EntryPayload::Blank => LocalEntryType::Empty,
            },
            group_id: self.group_id,
        };

        let data = {
            let mut serialized = Vec::new();
            // Serialize the entire entry for storage
            ciborium::into_writer(&entry, &mut serialized).unwrap_or_else(|e| {
                panic!("Failed to serialize log entry: {e}");
            });
            bytes::Bytes::from(serialized)
        };

        StorageLogEntry {
            index: entry.log_id.index,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            data,
            metadata,
        }
    }

    /// Convert storage log entry back to OpenRaft entry
    fn from_storage_log_entry(
        log_entry: StorageLogEntry<LocalLogMetadata>,
    ) -> Result<Entry<GroupConsensusTypeConfig>, StorageError<GroupConsensusTypeConfig>> {
        // Deserialize the full entry from the data field
        ciborium::from_reader(log_entry.data.as_ref()).map_err(|e| StorageError::read(&e))
    }

    /// Shutdown the storage gracefully
    pub async fn shutdown(&self) -> ConsensusResult<()> {
        debug!(
            group_id = ?self.group_id,
            "Shutting down group log storage"
        );

        // Clear cache
        self.log_cache.write().await.clear();

        // Flush any pending operations
        self.storage
            .flush()
            .await
            .map_err(|e| Error::storage(format!("Failed to flush storage: {e}")))?;

        // Shutdown the storage engine
        self.storage
            .shutdown()
            .await
            .map_err(|e| Error::storage(format!("Failed to shutdown storage: {e}")))?;

        Ok(())
    }
}

impl<S> Debug for GroupLogStorage<S>
where
    S: StorageEngine + LogStorage<LocalLogMetadata>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupLogStorage")
            .field("storage", &"<storage_engine>")
            .field("group_id", &self.group_id)
            .finish()
    }
}

impl<S> RaftLogReader<GroupConsensusTypeConfig> for GroupLogStorage<S>
where
    S: StorageEngine + LogStorage<LocalLogMetadata> + Clone + 'static,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<GroupConsensusTypeConfig>>, StorageError<GroupConsensusTypeConfig>> {
        let namespace = namespaces::logs();
        let caps = self.storage.capabilities();

        let storage_entries = if caps.efficient_range_scan {
            // Use optimized range scan
            self.storage
                .read_range(&namespace, range)
                .await
                .map_err(|e| {
                    StorageError::read(&std::io::Error::other(format!(
                        "Failed to read log range: {e}"
                    )))
                })?
        } else {
            // For storage without efficient range scan, read individual entries
            use std::ops::Bound;
            let start = match range.start_bound() {
                Bound::Included(&n) => n,
                Bound::Excluded(&n) => n + 1,
                Bound::Unbounded => 0,
            };
            let end = match range.end_bound() {
                Bound::Included(&n) => Some(n + 1),
                Bound::Excluded(&n) => Some(n),
                Bound::Unbounded => None,
            };

            let state = self.storage.get_log_state(&namespace).await.map_err(|e| {
                StorageError::read(&std::io::Error::other(format!(
                    "Failed to get log state: {e}"
                )))
            })?;

            if let Some(log_state) = state {
                let actual_end = end.unwrap_or(log_state.last_index + 1);
                let mut entries = Vec::new();

                for index in start..actual_end {
                    match self.storage.get_entry(&namespace, index).await {
                        Ok(Some(entry)) => entries.push(entry),
                        Ok(None) => break, // No more entries
                        Err(e) => {
                            return Err(StorageError::read(&std::io::Error::other(format!(
                                "Failed to read log entry: {e}"
                            ))));
                        }
                    }
                }
                entries
            } else {
                vec![]
            }
        };

        // Convert storage entries back to OpenRaft entries
        let mut entries = Vec::with_capacity(storage_entries.len());
        for storage_entry in storage_entries {
            let entry = Self::from_storage_log_entry(storage_entry)?;
            entries.push(entry);
        }

        Ok(entries)
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<GroupConsensusTypeConfig>>, StorageError<GroupConsensusTypeConfig>>
    {
        match self.storage.get(&namespaces::state(), &keys::vote()).await {
            Ok(Some(value)) => {
                let stored: StoredVote =
                    ciborium::from_reader(value.as_bytes()).map_err(|e| StorageError::read(&e))?;
                Ok(Some(stored.to_vote()))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(StorageError::read(&std::io::Error::other(format!(
                "Failed to read vote: {e}"
            )))),
        }
    }
}

impl<S> RaftLogStorage<GroupConsensusTypeConfig> for GroupLogStorage<S>
where
    S: StorageEngine + LogStorage<LocalLogMetadata> + Clone + 'static,
{
    type LogReader = Self;

    async fn save_vote(
        &mut self,
        vote: &Vote<GroupConsensusTypeConfig>,
    ) -> Result<(), StorageError<GroupConsensusTypeConfig>> {
        let stored = StoredVote::from(vote);
        let mut buffer = Vec::new();
        ciborium::into_writer(&stored, &mut buffer).map_err(|e| StorageError::write(&e))?;

        // Vote is critical for consensus correctness
        let hints = StorageHints {
            is_batch_write: false,
            access_pattern: crate::storage_backends::traits::AccessPattern::Random,
            allow_eventual_consistency: false,
            priority: Priority::Critical,
        };

        self.storage
            .put_with_hints(
                &namespaces::state(),
                keys::vote(),
                StorageValue::new(buffer),
                hints,
            )
            .await
            .map_err(|e| {
                StorageError::write(&std::io::Error::other(format!("Failed to save vote: {e}")))
            })?;

        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<GroupConsensusTypeConfig>,
    ) -> Result<(), StorageError<GroupConsensusTypeConfig>>
    where
        I: IntoIterator<Item = Entry<GroupConsensusTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let namespace = namespaces::logs();
        let caps = self.storage.capabilities();

        // Convert OpenRaft entries to storage log entries
        let storage_entries: Vec<StorageLogEntry<LocalLogMetadata>> = entries
            .into_iter()
            .map(|entry| {
                // Cache the entry
                self.log_cache
                    .blocking_write()
                    .insert(entry.log_id.index, entry.clone());
                self.to_storage_log_entry(&entry)
            })
            .collect();

        if storage_entries.is_empty() {
            callback.io_completed(Ok(()));
            return Ok(());
        }

        // Use enhanced batch operations if available
        let result = if caps.atomic_batches {
            // Use batch operations for atomicity
            self.storage
                .append_entries(&namespace, storage_entries)
                .await
                .map_err(|e| std::io::Error::other(format!("Failed to append log entries: {e}")))
        } else {
            // Fall back to individual operations
            let mut last_error = None;
            for entry in storage_entries {
                if let Err(e) = self.storage.append_entry(&namespace, entry).await {
                    last_error = Some(e);
                    break;
                }
            }
            if let Some(e) = last_error {
                Err(std::io::Error::other(format!(
                    "Failed to append log entry: {e}"
                )))
            } else {
                Ok(())
            }
        };

        match &result {
            Ok(_) => callback.io_completed(Ok(())),
            Err(e) => callback.io_completed(Err(std::io::Error::new(e.kind(), e.to_string()))),
        }
        result.map_err(|e| StorageError::write(&e))
    }

    async fn truncate(
        &mut self,
        log_id: LogId<GroupConsensusTypeConfig>,
    ) -> Result<(), StorageError<GroupConsensusTypeConfig>> {
        let namespace = namespaces::logs();

        // Clear cache entries after truncation point
        let mut cache = self.log_cache.write().await;
        cache.retain(|&index, _| index <= log_id.index);
        drop(cache);

        // Use LogStorage's truncate method for efficient truncation
        self.storage
            .truncate(&namespace, log_id.index)
            .await
            .map_err(|e| {
                StorageError::write(&std::io::Error::other(format!(
                    "Failed to truncate logs after index {}: {}",
                    log_id.index, e
                )))
            })
    }

    async fn purge(
        &mut self,
        log_id: LogId<GroupConsensusTypeConfig>,
    ) -> Result<(), StorageError<GroupConsensusTypeConfig>> {
        let namespace = namespaces::logs();

        // Clear cache entries before purge point
        let mut cache = self.log_cache.write().await;
        cache.retain(|&index, _| index > log_id.index);
        drop(cache);

        // Use LogStorage's purge method for efficient purging
        self.storage
            .purge(&namespace, log_id.index)
            .await
            .map_err(|e| {
                StorageError::write(&std::io::Error::other(format!(
                    "Failed to purge logs up to index {}: {}",
                    log_id.index, e
                )))
            })?;

        // Save the last purged log ID
        let key = keys::last_purged();
        let mut serialized = Vec::new();
        ciborium::into_writer(&log_id, &mut serialized).map_err(|e| StorageError::write(&e))?;

        self.storage
            .put(&namespaces::state(), key, StorageValue::new(serialized))
            .await
            .map_err(|e| {
                StorageError::write(&std::io::Error::other(format!(
                    "Failed to save last purged log ID: {e}"
                )))
            })?;

        Ok(())
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<GroupConsensusTypeConfig>, StorageError<GroupConsensusTypeConfig>> {
        let namespace = namespaces::logs();

        // Use LogStorage's get_log_state for O(1) performance
        let storage_state = self.storage.get_log_state(&namespace).await.map_err(|e| {
            StorageError::read(&std::io::Error::other(format!(
                "Failed to get log state: {e}"
            )))
        })?;

        let (last_log_id, last_purged_log_id) = if let Some(state) = storage_state {
            // Get the actual last log entry to extract the full LogId with leader info
            let last_log_id = if state.last_index > 0 {
                // Check cache first
                let cache = self.log_cache.read().await;
                if let Some(entry) = cache.get(&state.last_index) {
                    Some(entry.log_id.clone())
                } else {
                    drop(cache);
                    match self.storage.get_entry(&namespace, state.last_index).await {
                        Ok(Some(entry)) => {
                            let raft_entry = Self::from_storage_log_entry(entry)?;
                            Some(raft_entry.log_id)
                        }
                        _ => None,
                    }
                }
            } else {
                None
            };

            // Get last purged log ID from metadata
            let last_purged = match self
                .storage
                .get(&namespaces::state(), &keys::last_purged())
                .await
            {
                Ok(Some(value)) => {
                    let log_id: LogId<GroupConsensusTypeConfig> =
                        ciborium::from_reader(value.as_bytes())
                            .map_err(|e| StorageError::read(&e))?;
                    Some(log_id)
                }
                _ => None,
            };

            (last_log_id, last_purged)
        } else {
            (None, None)
        };

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }
}

/// Type alias for group log storage
pub type GroupLogStorageType<S> = Arc<GroupLogStorage<S>>;

#[cfg(test)]
mod tests {
    use crate::{
        ConsensusGroupId,
        core::group::storage::log_types::{LocalEntryType, LocalLogMetadata, StreamOperationType},
    };

    use proven_topology::NodeId;

    #[tokio::test]
    async fn test_log_metadata_serialization() {
        let metadata = LocalLogMetadata {
            term: 5,
            leader_node_id: NodeId::from_seed(1),
            entry_type: LocalEntryType::StreamOperation {
                stream_id: "test-stream".to_string(),
                operation: StreamOperationType::Publish,
            },
            group_id: ConsensusGroupId(42),
        };

        // Test serialization/deserialization
        let mut buffer = Vec::new();
        ciborium::into_writer(&metadata, &mut buffer).expect("Failed to serialize");

        let deserialized: LocalLogMetadata =
            ciborium::from_reader(&buffer[..]).expect("Failed to deserialize");

        assert_eq!(deserialized.term, metadata.term);
        assert_eq!(deserialized.group_id, metadata.group_id);
    }

    #[tokio::test]
    async fn test_local_entry_types() {
        let entry_types = vec![
            LocalEntryType::StreamOperation {
                stream_id: "stream1".to_string(),
                operation: StreamOperationType::Publish,
            },
            LocalEntryType::StreamManagement,
            LocalEntryType::MembershipChange,
            LocalEntryType::Migration,
            LocalEntryType::Empty,
        ];

        for entry_type in entry_types {
            let metadata = LocalLogMetadata {
                term: 1,
                leader_node_id: NodeId::from_seed(1),
                entry_type: entry_type.clone(),
                group_id: ConsensusGroupId(1),
            };

            // Test that all entry types can be serialized
            let mut buffer = Vec::new();
            ciborium::into_writer(&metadata, &mut buffer).expect("Failed to serialize");

            let deserialized: LocalLogMetadata =
                ciborium::from_reader(&buffer[..]).expect("Failed to deserialize");

            // Verify entry type matches
            match (entry_type, deserialized.entry_type) {
                (
                    LocalEntryType::StreamOperation { stream_id: s1, .. },
                    LocalEntryType::StreamOperation { stream_id: s2, .. },
                ) => {
                    assert_eq!(s1, s2);
                }
                (LocalEntryType::StreamManagement, LocalEntryType::StreamManagement) => {}
                (LocalEntryType::MembershipChange, LocalEntryType::MembershipChange) => {}
                (LocalEntryType::Migration, LocalEntryType::Migration) => {}
                (LocalEntryType::Empty, LocalEntryType::Empty) => {}
                _ => panic!("Entry type mismatch"),
            }
        }
    }
}
