use crate::local::state_machine::LocalState;
use crate::local::{LocalResponse, LocalStreamOperation, LocalTypeConfig};
use openraft::{
    Entry, EntryPayload, LogId, RaftLogReader, RaftSnapshotBuilder, SnapshotMeta, StorageError,
    StoredMembership, Vote,
    entry::RaftEntry,
    storage::{IOFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::RwLock;

/// In-memory storage for local consensus groups
#[derive(Clone)]
pub struct LocalMemoryStorage {
    /// The last applied log entry
    last_applied_log: Arc<RwLock<Option<LogId<LocalTypeConfig>>>>,

    /// The last membership configuration
    last_membership: Arc<RwLock<StoredMembership<LocalTypeConfig>>>,

    /// Log entries
    log: Arc<RwLock<BTreeMap<u64, Entry<LocalTypeConfig>>>>,

    /// Vote
    vote: Arc<RwLock<Option<Vote<LocalTypeConfig>>>>,

    /// Snapshot
    snapshot: Arc<RwLock<Option<LocalSnapshot>>>,

    /// The state machine
    state_machine: Arc<RwLock<LocalState>>,

    /// Last purged log ID
    last_purged: Arc<RwLock<Option<LogId<LocalTypeConfig>>>>,
}

/// A snapshot of the local state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalSnapshot {
    /// The metadata of the snapshot
    pub meta: SnapshotMeta<LocalTypeConfig>,
    /// The data of the snapshot
    pub data: Vec<u8>,
}

impl LocalMemoryStorage {
    /// Create a new memory storage for a local consensus group
    pub fn new(group_id: crate::allocation::ConsensusGroupId) -> Self {
        Self {
            last_applied_log: Arc::new(RwLock::new(None)),
            last_membership: Arc::new(RwLock::new(StoredMembership::default())),
            log: Arc::new(RwLock::new(BTreeMap::new())),
            vote: Arc::new(RwLock::new(None)),
            snapshot: Arc::new(RwLock::new(None)),
            state_machine: Arc::new(RwLock::new(LocalState::new(group_id))),
            last_purged: Arc::new(RwLock::new(None)),
        }
    }
}

impl RaftLogReader<LocalTypeConfig> for LocalMemoryStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<LocalTypeConfig>>, StorageError<LocalTypeConfig>> {
        let log = self.log.read().await;
        let entries = log.range(range).map(|(_, entry)| entry.clone()).collect();
        Ok(entries)
    }

    async fn read_vote(
        &mut self,
    ) -> Result<Option<Vote<LocalTypeConfig>>, StorageError<LocalTypeConfig>> {
        Ok(self.vote.read().await.clone())
    }
}

impl RaftLogStorage<LocalTypeConfig> for LocalMemoryStorage {
    type LogReader = Self;

    async fn save_vote(
        &mut self,
        vote: &Vote<LocalTypeConfig>,
    ) -> Result<(), StorageError<LocalTypeConfig>> {
        *self.vote.write().await = Some(vote.clone());
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
        log_id: LogId<LocalTypeConfig>,
    ) -> Result<(), StorageError<LocalTypeConfig>> {
        let mut log = self.log.write().await;
        log.retain(|&index, _| index <= log_id.index);
        Ok(())
    }

    async fn purge(
        &mut self,
        log_id: LogId<LocalTypeConfig>,
    ) -> Result<(), StorageError<LocalTypeConfig>> {
        let mut log = self.log.write().await;
        let mut last_purged = self.last_purged.write().await;

        log.retain(|&index, _| index > log_id.index);
        *last_purged = Some(log_id);

        Ok(())
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<LocalTypeConfig>, StorageError<LocalTypeConfig>> {
        let log = self.log.read().await;
        let last_log_id = log.values().last().map(|entry| entry.log_id().clone());
        let last_purged_log_id = self.last_purged.read().await.clone();

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }
}

impl RaftStateMachine<LocalTypeConfig> for LocalMemoryStorage {
    type SnapshotBuilder = LocalSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<LocalTypeConfig>>,
            StoredMembership<LocalTypeConfig>,
        ),
        StorageError<LocalTypeConfig>,
    > {
        let last_applied = self.last_applied_log.read().await.clone();
        let last_membership = self.last_membership.read().await.clone();
        Ok((last_applied, last_membership))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<LocalResponse>, StorageError<LocalTypeConfig>>
    where
        I: IntoIterator<Item = Entry<LocalTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut state_machine = self.state_machine.write().await;
        let mut responses = Vec::new();

        for entry in entries {
            let mut resp = LocalResponse {
                success: false,
                sequence: None,
                error: None,
                checkpoint_data: None,
            };

            match &entry.payload {
                EntryPayload::Normal(req) => match &req.operation {
                    LocalStreamOperation::PublishToStream {
                        stream,
                        data,
                        metadata,
                    } => {
                        match state_machine.publish_message(stream, data.clone(), metadata.clone())
                        {
                            Ok(seq) => {
                                resp.success = true;
                                resp.sequence = Some(seq);
                            }
                            Err(e) => {
                                resp.error = Some(e);
                            }
                        }
                    }
                    LocalStreamOperation::CreateStreamForMigration {
                        stream_name,
                        source_group,
                    } => {
                        state_machine
                            .create_stream_for_migration(stream_name.clone(), *source_group);
                        resp.success = true;
                    }
                    LocalStreamOperation::GetStreamCheckpoint { stream_name } => {
                        match state_machine.create_migration_checkpoint(stream_name) {
                            Ok(checkpoint) => {
                                resp.success = true;
                                resp.sequence = Some(checkpoint.sequence);
                                // Serialize and return the checkpoint data
                                match serde_json::to_vec(&checkpoint) {
                                    Ok(data) => {
                                        resp.checkpoint_data = Some(bytes::Bytes::from(data));
                                    }
                                    Err(e) => {
                                        resp.error =
                                            Some(format!("Failed to serialize checkpoint: {}", e));
                                        resp.success = false;
                                    }
                                }
                            }
                            Err(e) => {
                                resp.error = Some(e);
                            }
                        }
                    }
                    LocalStreamOperation::GetIncrementalCheckpoint {
                        stream_name,
                        since_sequence,
                    } => match state_machine
                        .create_incremental_checkpoint(stream_name, *since_sequence)
                    {
                        Ok(checkpoint) => {
                            resp.success = true;
                            resp.sequence = Some(checkpoint.sequence);
                            // Serialize and return the incremental checkpoint data
                            match serde_json::to_vec(&checkpoint) {
                                Ok(data) => {
                                    resp.checkpoint_data = Some(bytes::Bytes::from(data));
                                }
                                Err(e) => {
                                    resp.error =
                                        Some(format!("Failed to serialize checkpoint: {}", e));
                                    resp.success = false;
                                }
                            }
                        }
                        Err(e) => {
                            resp.error = Some(e);
                        }
                    },
                    LocalStreamOperation::ApplyMigrationCheckpoint { checkpoint } => {
                        match state_machine.apply_migration_checkpoint(checkpoint) {
                            Ok(()) => {
                                resp.success = true;
                            }
                            Err(e) => {
                                resp.error = Some(e);
                            }
                        }
                    }
                    LocalStreamOperation::ApplyIncrementalCheckpoint { checkpoint } => {
                        match state_machine.apply_incremental_checkpoint(checkpoint) {
                            Ok(()) => {
                                resp.success = true;
                            }
                            Err(e) => {
                                resp.error = Some(e);
                            }
                        }
                    }
                    LocalStreamOperation::PauseStream { stream_name } => {
                        match state_machine.pause_stream(stream_name) {
                            Ok(last_seq) => {
                                resp.success = true;
                                resp.sequence = Some(last_seq);
                            }
                            Err(e) => {
                                resp.error = Some(e);
                            }
                        }
                    }
                    LocalStreamOperation::ResumeStream { stream_name } => {
                        match state_machine.resume_stream(stream_name) {
                            Ok(applied_sequences) => {
                                resp.success = true;
                                // Return the number of operations applied
                                resp.sequence = Some(applied_sequences.len() as u64);
                            }
                            Err(e) => {
                                resp.error = Some(e);
                            }
                        }
                    }
                    LocalStreamOperation::RemoveStream { stream_name } => {
                        match state_machine.remove_stream_for_migration(stream_name) {
                            Ok(()) => {
                                resp.success = true;
                            }
                            Err(e) => {
                                resp.error = Some(e);
                            }
                        }
                    }
                    LocalStreamOperation::GetMetrics => {
                        let metrics = state_machine.get_metrics();
                        match serde_json::to_vec(&metrics) {
                            Ok(data) => {
                                resp.success = true;
                                resp.checkpoint_data = Some(bytes::Bytes::from(data));
                            }
                            Err(e) => {
                                resp.error = Some(format!("Failed to serialize metrics: {}", e));
                            }
                        }
                    }
                    LocalStreamOperation::CleanupPendingOperations { max_age_secs } => {
                        let cleaned = state_machine.cleanup_old_pending_operations(*max_age_secs);
                        resp.success = true;
                        resp.sequence = Some(cleaned as u64);
                    }
                    _ => {
                        resp.error = Some("Operation not implemented".to_string());
                    }
                },
                EntryPayload::Membership(membership) => {
                    let mut last_membership = self.last_membership.write().await;
                    *last_membership =
                        StoredMembership::new(Some(entry.log_id().clone()), membership.clone());
                    resp.success = true;
                }
                _ => {}
            }

            responses.push(resp);

            let mut last_applied = self.last_applied_log.write().await;
            *last_applied = Some(entry.log_id().clone());
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let last_applied = self.last_applied_log.read().await.clone();
        let last_membership = self.last_membership.read().await.clone();
        let state_machine = self.state_machine.clone();

        LocalSnapshotBuilder {
            last_applied,
            last_membership,
            state_machine,
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
        let data = snapshot.into_inner();

        let snapshot_data: LocalState = serde_json::from_slice(&data)
            .map_err(|e| StorageError::read_snapshot(Some(meta.signature()), &e))?;

        let mut state_machine = self.state_machine.write().await;
        *state_machine = snapshot_data;

        let mut last_applied = self.last_applied_log.write().await;
        *last_applied = meta.last_log_id.clone();

        let mut last_membership = self.last_membership.write().await;
        *last_membership = meta.last_membership.clone();

        let mut snapshot = self.snapshot.write().await;
        *snapshot = Some(LocalSnapshot {
            meta: meta.clone(),
            data,
        });

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<LocalTypeConfig>>, StorageError<LocalTypeConfig>> {
        match self.snapshot.read().await.clone() {
            Some(snapshot) => {
                let data = Cursor::new(snapshot.data.clone());
                Ok(Some(Snapshot {
                    meta: snapshot.meta,
                    snapshot: data,
                }))
            }
            None => Ok(None),
        }
    }
}

/// Snapshot builder for local storage
pub struct LocalSnapshotBuilder {
    last_applied: Option<LogId<LocalTypeConfig>>,
    last_membership: StoredMembership<LocalTypeConfig>,
    state_machine: Arc<RwLock<LocalState>>,
}

impl RaftSnapshotBuilder<LocalTypeConfig> for LocalSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<LocalTypeConfig>, StorageError<LocalTypeConfig>> {
        let state_machine = self.state_machine.read().await;
        let data = serde_json::to_vec(&state_machine.clone())
            .map_err(|e| StorageError::write_snapshot(None, &e))?;

        let last_applied = self.last_applied.clone();
        let last_membership = self.last_membership.clone();

        let snapshot_id = format!(
            "{}-{}",
            last_applied.as_ref().map(|id| id.index).unwrap_or(0),
            chrono::Utc::now().timestamp_millis()
        );

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            snapshot_id,
            last_membership,
        };

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        })
    }
}
