//! RaftStateMachine implementation for RocksDB

use std::io::Cursor;

use openraft::{
    Entry, EntryPayload, LogId, RaftSnapshotBuilder, SnapshotMeta, StorageError, StoredMembership,
    entry::RaftEntry,
    storage::{RaftStateMachine, Snapshot},
};
use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::local::state_machine::LocalState;
use crate::local::{LocalResponse, LocalTypeConfig};
use crate::{
    error::{ConsensusResult, Error},
    local::LocalStreamOperation,
};

use super::storage::{CF_DEFAULT, CF_SNAPSHOTS, LocalRocksDBStorage};

/// Key prefixes for default column family
const KEY_PREFIX_LAST_APPLIED: &str = "last_applied";
const KEY_PREFIX_LAST_MEMBERSHIP: &str = "last_membership";

/// Snapshot data structure for RocksDB storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksDBSnapshot {
    /// The metadata of the snapshot
    pub meta: SnapshotMeta<LocalTypeConfig>,
    /// The serialized state machine data
    pub data: Vec<u8>,
}

/// Snapshot builder for RocksDB storage
pub struct RocksDBSnapshotBuilder {
    storage: LocalRocksDBStorage,
}

impl LocalRocksDBStorage {
    /// Get last applied log ID
    async fn get_last_applied(&self) -> ConsensusResult<Option<LogId<LocalTypeConfig>>> {
        let cf_default = self.get_cf(CF_DEFAULT).await?;
        let key = KEY_PREFIX_LAST_APPLIED.as_bytes();

        if let Some(value) = self
            .db
            .get_cf(&cf_default, key)
            .map_err(|e| Error::Storage(format!("Failed to get last applied: {}", e)))?
        {
            let log_id: LogId<LocalTypeConfig> = serde_json::from_slice(&value).map_err(|e| {
                Error::Storage(format!("Failed to deserialize last applied: {}", e))
            })?;
            Ok(Some(log_id))
        } else {
            Ok(None)
        }
    }

    /// Save last applied log ID
    async fn save_last_applied(&self, log_id: &LogId<LocalTypeConfig>) -> ConsensusResult<()> {
        let cf_default = self.get_cf(CF_DEFAULT).await?;
        let key = KEY_PREFIX_LAST_APPLIED.as_bytes();
        let value = serde_json::to_vec(log_id)
            .map_err(|e| Error::Storage(format!("Failed to serialize last applied: {}", e)))?;

        self.db
            .put_cf(&cf_default, key, value)
            .map_err(|e| Error::Storage(format!("Failed to save last applied: {}", e)))?;

        Ok(())
    }

    /// Get last membership
    async fn get_last_membership(&self) -> ConsensusResult<StoredMembership<LocalTypeConfig>> {
        let cf_default = self.get_cf(CF_DEFAULT).await?;
        let key = KEY_PREFIX_LAST_MEMBERSHIP.as_bytes();

        if let Some(value) = self
            .db
            .get_cf(&cf_default, key)
            .map_err(|e| Error::Storage(format!("Failed to get last membership: {}", e)))?
        {
            let membership: StoredMembership<LocalTypeConfig> = serde_json::from_slice(&value)
                .map_err(|e| {
                    Error::Storage(format!("Failed to deserialize last membership: {}", e))
                })?;
            Ok(membership)
        } else {
            Ok(StoredMembership::default())
        }
    }

    /// Save last membership
    async fn save_last_membership(
        &self,
        membership: &StoredMembership<LocalTypeConfig>,
    ) -> ConsensusResult<()> {
        let cf_default = self.get_cf(CF_DEFAULT).await?;
        let key = KEY_PREFIX_LAST_MEMBERSHIP.as_bytes();
        let value = serde_json::to_vec(membership)
            .map_err(|e| Error::Storage(format!("Failed to serialize last membership: {}", e)))?;

        self.db
            .put_cf(&cf_default, key, value)
            .map_err(|e| Error::Storage(format!("Failed to save last membership: {}", e)))?;

        Ok(())
    }

    /// Apply a single entry to the state machine
    async fn apply_entry(&self, entry: &Entry<LocalTypeConfig>) -> ConsensusResult<LocalResponse> {
        let mut state_machine = self.state_machine.write().await;
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
                } => match state_machine.publish_message(stream, data.clone(), metadata.clone()) {
                    Ok(seq) => {
                        resp.success = true;
                        resp.sequence = Some(seq);
                    }
                    Err(e) => {
                        resp.error = Some(e);
                    }
                },
                LocalStreamOperation::CreateStreamForMigration {
                    stream_name,
                    source_group,
                } => {
                    state_machine.create_stream_for_migration(stream_name.clone(), *source_group);
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
                } => {
                    match state_machine.create_incremental_checkpoint(stream_name, *since_sequence)
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
                    }
                }
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
            EntryPayload::Membership(_membership) => {
                // Membership changes are handled separately
                resp.success = true;
            }
            _ => {
                // Blank entries
                resp.success = true;
            }
        }

        Ok(resp)
    }
}

impl RaftStateMachine<LocalTypeConfig> for LocalRocksDBStorage {
    type SnapshotBuilder = RocksDBSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<LocalTypeConfig>>,
            StoredMembership<LocalTypeConfig>,
        ),
        StorageError<LocalTypeConfig>,
    > {
        let last_applied = self
            .get_last_applied()
            .await
            .map_err(|e| StorageError::read(&e))?;
        let last_membership = self
            .get_last_membership()
            .await
            .map_err(|e| StorageError::read(&e))?;

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
        let mut responses = Vec::new();

        for entry in entries {
            let log_id = entry.log_id().clone();

            // Apply entry to state machine
            let response = self
                .apply_entry(&entry)
                .await
                .map_err(|e| StorageError::apply(log_id.clone(), &e))?;

            // Update last applied log ID
            self.save_last_applied(&log_id)
                .await
                .map_err(|e| StorageError::apply(log_id.clone(), &e))?;

            // Handle membership changes
            if let EntryPayload::Membership(membership) = &entry.payload {
                let stored_membership =
                    StoredMembership::new(Some(log_id.clone()), membership.clone());
                self.save_last_membership(&stored_membership)
                    .await
                    .map_err(|e| StorageError::apply(log_id, &e))?;
            }

            responses.push(response);
        }

        debug!("Applied {} entries to state machine", responses.len());
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        RocksDBSnapshotBuilder {
            storage: self.clone(),
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
        info!(
            "Installing snapshot with last_log_id: {:?}",
            meta.last_log_id
        );

        let data = snapshot.into_inner();
        if data.is_empty() {
            return Err(StorageError::read_snapshot(
                None,
                &Error::Storage("Empty snapshot data".to_string()),
            ));
        }

        // Deserialize the state machine data
        let snapshot_state: LocalState = serde_json::from_slice(&data)
            .map_err(|e| StorageError::read_snapshot(Some(meta.signature()), &e))?;

        // Update state machine
        let mut state_machine = self.state_machine.write().await;
        *state_machine = snapshot_state;
        drop(state_machine);

        // Update metadata in a batch
        let mut batch = WriteBatch::default();
        let cf_default = self
            .get_cf(CF_DEFAULT)
            .await
            .map_err(|e| StorageError::write(&e))?;

        // Save last applied log ID
        if let Some(ref log_id) = meta.last_log_id {
            let key = KEY_PREFIX_LAST_APPLIED.as_bytes();
            let value = serde_json::to_vec(log_id).map_err(|e| StorageError::write(&e))?;
            batch.put_cf(&cf_default, key, value);
        }

        // Save last membership
        let key = KEY_PREFIX_LAST_MEMBERSHIP.as_bytes();
        let value =
            serde_json::to_vec(&meta.last_membership).map_err(|e| StorageError::write(&e))?;
        batch.put_cf(&cf_default, key, value);

        // Store snapshot in snapshots column family
        let cf_snapshots = self
            .get_cf(CF_SNAPSHOTS)
            .await
            .map_err(|e| StorageError::write(&e))?;
        let snapshot_key = format!("snapshot_{}", meta.snapshot_id);
        let snapshot_data = RocksDBSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        let snapshot_value =
            serde_json::to_vec(&snapshot_data).map_err(|e| StorageError::write(&e))?;
        batch.put_cf(&cf_snapshots, snapshot_key.as_bytes(), snapshot_value);

        // Write batch atomically
        self.db.write(batch).map_err(|e| StorageError::write(&e))?;

        info!("Successfully installed snapshot");
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<LocalTypeConfig>>, StorageError<LocalTypeConfig>> {
        // Get current state
        let last_applied = self
            .get_last_applied()
            .await
            .map_err(|e| StorageError::read(&e))?;
        let last_membership = self
            .get_last_membership()
            .await
            .map_err(|e| StorageError::read(&e))?;

        // Serialize current state machine
        let state_machine = self.state_machine.read().await;
        let data = serde_json::to_vec(&state_machine.clone())
            .map_err(|e| StorageError::write_snapshot(None, &e))?;

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

        Ok(Some(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        }))
    }
}

impl RaftSnapshotBuilder<LocalTypeConfig> for RocksDBSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<LocalTypeConfig>, StorageError<LocalTypeConfig>> {
        self.storage
            .get_current_snapshot()
            .await
            .map(|opt| opt.expect("get_current_snapshot should always return Some for RocksDB"))
    }
}
