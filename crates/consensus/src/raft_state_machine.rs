//! OpenRaft state machine implementation for consensus messaging.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use openraft::entry::RaftEntry;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{Entry, LogId, SnapshotMeta, StorageError, StoredMembership};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::consensus_manager::{MessagingResponse, StreamStore, TypeConfig};

/// Snapshot data for the state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineSnapshot {
    /// All stream data at the time of snapshot
    pub streams: BTreeMap<String, BTreeMap<u64, Vec<u8>>>,
    /// Sequence counters for each stream
    pub sequences: BTreeMap<String, u64>,
    /// Last applied log ID
    pub last_applied: Option<LogId<TypeConfig>>,
}

/// `OpenRaft` state machine implementation using our `StreamStore`
#[derive(Debug, Clone)]
pub struct ConsensusStateMachine {
    /// The underlying stream store that handles our business logic
    store: Arc<StreamStore>,
    /// Last applied log ID for tracking state machine progress
    last_applied: Arc<RwLock<Option<LogId<TypeConfig>>>>,
    /// Last snapshot metadata
    last_snapshot: Arc<RwLock<Option<LogId<TypeConfig>>>>,
}

impl ConsensusStateMachine {
    /// Create a new state machine with the provided stream store
    #[must_use]
    pub fn new(store: Arc<StreamStore>) -> Self {
        Self {
            store,
            last_applied: Arc::new(RwLock::new(None)),
            last_snapshot: Arc::new(RwLock::new(None)),
        }
    }
}

impl RaftStateMachine<TypeConfig> for ConsensusStateMachine {
    type SnapshotBuilder = ConsensusSnapshotBuilder;

    /// Get the current applied state and snapshot metadata
    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<TypeConfig>>, StoredMembership<TypeConfig>), StorageError<TypeConfig>>
    {
        let last_applied = *self.last_applied.read().await;
        let _last_snapshot = *self.last_snapshot.read().await;

        debug!("State machine applied_state: applied={:?}", last_applied);

        // Return empty membership for now - in a real implementation this would track cluster membership
        let membership = StoredMembership::default();
        Ok((last_applied, membership))
    }

    /// Apply a batch of committed log entries to the state machine
    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<MessagingResponse>, StorageError<TypeConfig>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();

        for entry in entries {
            let log_id = entry.log_id();
            debug!("Applying log entry at index {}", log_id.index);

            // Update last applied
            *self.last_applied.write().await = Some(log_id);

            match entry.payload {
                openraft::EntryPayload::Blank => {
                    // No-op for blank entries (usually leadership establishment)
                    responses.push(MessagingResponse {
                        sequence: log_id.index,
                        success: true,
                        error: None,
                    });
                }
                openraft::EntryPayload::Normal(request) => {
                    // Apply the messaging request through our stream store
                    match self.store.apply_request(&request) {
                        Ok(response) => {
                            info!("Applied request: sequence {}", response.sequence);
                            responses.push(response);
                        }
                        Err(e) => {
                            warn!("Failed to apply request: {}", e);
                            responses.push(MessagingResponse {
                                sequence: log_id.index,
                                success: false,
                                error: Some(format!("Application failed: {e}")),
                            });
                        }
                    }
                }
                openraft::EntryPayload::Membership(ref membership) => {
                    // Handle membership changes - these are automatically applied by OpenRaft
                    info!("Applied membership change: {:?}", membership);
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

    /// Begin receiving a snapshot from the leader
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Cursor<Vec<u8>>, StorageError<TypeConfig>> {
        info!("Beginning to receive snapshot");
        // Return a cursor that can accept snapshot data
        Ok(Cursor::new(Vec::new()))
    }

    /// Install a snapshot
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<TypeConfig>,
        _snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), StorageError<TypeConfig>> {
        info!("Installing snapshot: {:?}", meta);

        // In a real implementation, we would read from the snapshot stream
        // and restore our state machine from it
        warn!("Snapshot installation not fully implemented - accepting snapshot metadata only");

        // Update our last applied and snapshot tracking
        *self.last_applied.write().await = meta.last_log_id;
        *self.last_snapshot.write().await = meta.last_log_id;

        Ok(())
    }

    /// Get the current snapshot
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<openraft::Snapshot<TypeConfig>>, StorageError<TypeConfig>> {
        let last_applied = *self.last_applied.read().await;

        if last_applied.is_none() {
            // No snapshot available yet
            return Ok(None);
        }

        info!("Creating current snapshot");

        // Create a simple snapshot
        let snapshot_data = StateMachineSnapshot {
            streams: BTreeMap::new(), // Simplified - would contain actual stream data
            sequences: BTreeMap::new(),
            last_applied,
        };

        let snapshot_bytes =
            serde_json::to_vec(&snapshot_data).map_err(|e| StorageError::read_state_machine(&e))?;

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: StoredMembership::default(),
            snapshot_id: format!("snapshot-{}", last_applied.map_or(0, |id| id.index)),
        };

        let snapshot = openraft::Snapshot {
            meta,
            snapshot: Cursor::new(snapshot_bytes),
        };

        Ok(Some(snapshot))
    }

    /// Get a snapshot builder for creating snapshots
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        info!("Getting snapshot builder");
        ConsensusSnapshotBuilder::new(self.store.clone(), *self.last_applied.read().await)
    }
}

/// Snapshot builder implementation
#[derive(Debug)]
pub struct ConsensusSnapshotBuilder {
    #[allow(dead_code)]
    store: Arc<StreamStore>,
    last_applied: Option<LogId<TypeConfig>>,
}

impl ConsensusSnapshotBuilder {
    /// Create a new snapshot builder
    #[must_use]
    pub const fn new(store: Arc<StreamStore>, last_applied: Option<LogId<TypeConfig>>) -> Self {
        Self {
            store,
            last_applied,
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for ConsensusSnapshotBuilder {
    /// Build a snapshot of the current state machine
    async fn build_snapshot(
        &mut self,
    ) -> Result<openraft::Snapshot<TypeConfig>, StorageError<TypeConfig>> {
        let snapshot_id = format!("snapshot-{}", self.last_applied.map_or(0, |id| id.index));

        info!("Building snapshot: {}", snapshot_id);

        // Create the snapshot data
        let snapshot_data = StateMachineSnapshot {
            streams: BTreeMap::new(), // Simplified - would export actual stream data
            sequences: BTreeMap::new(),
            last_applied: self.last_applied,
        };

        // Serialize the snapshot
        let snapshot_bytes =
            serde_json::to_vec(&snapshot_data).map_err(|e| StorageError::read_state_machine(&e))?;

        // Create the snapshot metadata
        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: StoredMembership::default(),
            snapshot_id,
        };

        // Create the snapshot with a cursor over the serialized data
        let snapshot = openraft::Snapshot {
            meta,
            snapshot: Cursor::new(snapshot_bytes),
        };

        info!("Successfully built snapshot");
        Ok(snapshot)
    }
}
