//! State machine implementation for global consensus
//!
//! This module provides the OpenRaft state machine implementation
//! using the new GlobalState and operations structure.

use crate::core::global::{GlobalConsensusTypeConfig, GlobalState};
use crate::operations::handlers::{
    GlobalOperationResponse, OperationContext, OperationHandlerRegistry,
};
use openraft::{
    Entry, EntryPayload, LogId, RaftSnapshotBuilder, SnapshotMeta, StorageError, StoredMembership,
};
use proven_topology::NodeId;
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

// Re-export ConsensusGroupInfo from global_state

/// State machine for global consensus using operations
pub struct GlobalStateMachine {
    /// The global state
    state: Arc<GlobalState>,
    /// Operation handler registry
    handler_registry: Arc<OperationHandlerRegistry>,
    /// Last applied log ID
    last_applied_log: Arc<RwLock<Option<LogId<GlobalConsensusTypeConfig>>>>,
    /// Last membership config
    last_membership: Arc<RwLock<StoredMembership<GlobalConsensusTypeConfig>>>,
}

impl GlobalStateMachine {
    /// Create a new state machine
    pub fn new(state: Arc<GlobalState>) -> Self {
        // Create handler registry with default handlers
        let handler_registry = Arc::new(OperationHandlerRegistry::with_defaults(None, 10));

        Self {
            state,
            handler_registry,
            last_applied_log: Arc::new(RwLock::new(None)),
            last_membership: Arc::new(RwLock::new(StoredMembership::default())),
        }
    }

    /// Create a new state machine with custom handler registry
    pub fn with_registry(
        state: Arc<GlobalState>,
        handler_registry: Arc<OperationHandlerRegistry>,
    ) -> Self {
        Self {
            state,
            handler_registry,
            last_applied_log: Arc::new(RwLock::new(None)),
            last_membership: Arc::new(RwLock::new(StoredMembership::default())),
        }
    }

    /// Get reference to the state
    pub fn state(&self) -> Arc<GlobalState> {
        self.state.clone()
    }
}

/// Snapshot builder for global state machine
pub struct GlobalSnapshotBuilder {
    state: Arc<GlobalState>,
    last_applied_log: Option<LogId<GlobalConsensusTypeConfig>>,
    last_membership: StoredMembership<GlobalConsensusTypeConfig>,
}

impl GlobalSnapshotBuilder {
    fn new(
        state: Arc<GlobalState>,
        last_applied_log: Option<LogId<GlobalConsensusTypeConfig>>,
        last_membership: StoredMembership<GlobalConsensusTypeConfig>,
    ) -> Self {
        Self {
            state,
            last_applied_log,
            last_membership,
        }
    }
}

/// Snapshot data for global state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GlobalSnapshotData {
    last_applied_log: Option<LogId<GlobalConsensusTypeConfig>>,
    last_membership: StoredMembership<GlobalConsensusTypeConfig>,
    state: serde_json::Value, // GlobalState snapshot
}

impl RaftSnapshotBuilder<GlobalConsensusTypeConfig> for GlobalSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<
        openraft::storage::Snapshot<GlobalConsensusTypeConfig>,
        StorageError<GlobalConsensusTypeConfig>,
    > {
        // Serialize the current state
        // TODO: Implement proper state snapshot serialization
        let state_snapshot = serde_json::json!({
            "streams": [],
            "nodes": [],
            "groups": []
        });

        let snapshot_data = GlobalSnapshotData {
            last_applied_log: self.last_applied_log.clone(),
            last_membership: self.last_membership.clone(),
            state: state_snapshot,
        };

        let mut buffer = Vec::new();
        ciborium::into_writer(&snapshot_data, &mut buffer)
            .map_err(|e| StorageError::write_state_machine(&e))?;

        let snapshot_id = format!(
            "global-snapshot-{}-{}",
            self.last_applied_log
                .as_ref()
                .map(|log| log.index.to_string())
                .unwrap_or_else(|| "0".to_string()),
            chrono::Utc::now().timestamp(),
        );

        Ok(openraft::storage::Snapshot {
            meta: SnapshotMeta {
                last_log_id: self.last_applied_log.clone(),
                last_membership: self.last_membership.clone(),
                snapshot_id,
            },
            snapshot: Cursor::new(buffer),
        })
    }
}

impl openraft::storage::RaftStateMachine<GlobalConsensusTypeConfig> for GlobalStateMachine {
    type SnapshotBuilder = GlobalSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<GlobalConsensusTypeConfig>>,
            StoredMembership<GlobalConsensusTypeConfig>,
        ),
        StorageError<GlobalConsensusTypeConfig>,
    > {
        let last_applied = self.last_applied_log.read().await.clone();
        let membership = self.last_membership.read().await.clone();
        Ok((last_applied, membership))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<GlobalOperationResponse>, StorageError<GlobalConsensusTypeConfig>>
    where
        I: IntoIterator<Item = Entry<GlobalConsensusTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();

        for entry in entries {
            let mut last_applied = self.last_applied_log.write().await;
            *last_applied = Some(entry.log_id.clone());

            match &entry.payload {
                EntryPayload::Blank => {
                    // For blank entries, create a simple success response
                    responses.push(GlobalOperationResponse::StreamManagement(
                        crate::operations::handlers::StreamManagementResponse::ConfigUpdated {
                            sequence: entry.log_id.index,
                            stream_name: String::new(),
                            changes: vec!["Blank entry processed".to_string()],
                        },
                    ));
                }
                EntryPayload::Normal(request) => {
                    // Create operation context
                    // Note: In a real system, we'd extract the proposer from the log entry metadata
                    let context = OperationContext::new(
                        entry.log_id.index,
                        NodeId::from_seed(0), // Default node ID - in production this would come from entry metadata
                        true, // State machine applies are always from committed entries
                    );

                    // Use handler registry to process the operation
                    let handler_response = self
                        .handler_registry
                        .handle_operation(request, &self.state, &context)
                        .await;

                    // Add handler response to the list
                    match handler_response {
                        Ok(op_response) => {
                            responses.push(op_response);
                        }
                        Err(e) => {
                            // Create a failed response
                            responses.push(GlobalOperationResponse::StreamManagement(
                                crate::operations::handlers::StreamManagementResponse::Failed {
                                    operation: format!("{request:?}"),
                                    reason: e.to_string(),
                                },
                            ));
                        }
                    }
                }
                EntryPayload::Membership(membership) => {
                    let mut last_membership = self.last_membership.write().await;
                    *last_membership =
                        StoredMembership::new(Some(entry.log_id.clone()), membership.clone());

                    // For membership changes, create a node operation response
                    responses.push(GlobalOperationResponse::Node(
                        crate::operations::handlers::NodeOperationResponse::AssignedToGroup {
                            sequence: entry.log_id.index,
                            node_id: NodeId::from_seed(0), // Placeholder
                            group_id: crate::ConsensusGroupId::initial(),
                            total_groups: 1,
                        },
                    ));
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let last_applied = self.last_applied_log.read().await.clone();
        let last_membership = self.last_membership.read().await.clone();

        GlobalSnapshotBuilder::new(self.state.clone(), last_applied, last_membership)
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<GlobalConsensusTypeConfig>,
        _snapshot: <GlobalConsensusTypeConfig as openraft::RaftTypeConfig>::SnapshotData,
    ) -> Result<(), StorageError<GlobalConsensusTypeConfig>> {
        debug!("Installing snapshot with id: {}", meta.snapshot_id);

        // Update last applied log
        if let Some(log_id) = &meta.last_log_id {
            let mut last_applied = self.last_applied_log.write().await;
            *last_applied = Some(log_id.clone());
        }

        // Update membership
        let mut last_membership = self.last_membership.write().await;
        *last_membership = meta.last_membership.clone();

        // In a real implementation, we'd deserialize and restore the full state
        // For now, we just log the installation
        debug!("Snapshot installed successfully");

        Ok(())
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<
        <GlobalConsensusTypeConfig as openraft::RaftTypeConfig>::SnapshotData,
        StorageError<GlobalConsensusTypeConfig>,
    > {
        Ok(Cursor::new(Vec::new()))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<
        Option<openraft::storage::Snapshot<GlobalConsensusTypeConfig>>,
        StorageError<GlobalConsensusTypeConfig>,
    > {
        // For now, return None - in production, we'd check if we have a stored snapshot
        Ok(None)
    }
}

/// Snapshot data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotData {
    /// Last applied log ID
    last_applied_log: Option<LogId<GlobalConsensusTypeConfig>>,
    /// Number of consensus groups
    group_count: usize,
    // In a real implementation, we'd include:
    // - All stream configurations
    // - All consensus group information
    // - Subject routing information
    // - etc.
}
