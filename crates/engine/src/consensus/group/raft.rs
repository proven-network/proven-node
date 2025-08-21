//! Group consensus Raft integration
//!
//! This module integrates the group state with OpenRaft for consensus.
//! It uses separated storage and state machine components.

use std::{marker::PhantomData, sync::Arc};

use openraft::{
    Config, Entry, Raft, RaftNetworkFactory,
    async_runtime::watch::WatchReceiver,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use proven_storage::{LogStorage, StorageNamespace};
use proven_topology::NodeId;

use crate::error::{ConsensusResult, Error, ErrorKind};
use crate::foundation::{
    GroupState, GroupStateWriter,
    traits::ConsensusLayer,
    types::{ConsensusGroupId, ConsensusRole, OperationId, Term},
};

use super::callbacks::GroupConsensusCallbacks;
use super::dispatcher::GroupCallbackDispatcher;
use super::operations::{GroupOperation, GroupOperationHandler};
use super::snapshot::GroupSnapshot;
use super::state_machine::GroupStateMachine;
use super::storage::GroupRaftLogStorage;
use super::types::{GroupRequest, GroupResponse};

/// Trait for handling Raft RPC messages
#[async_trait::async_trait]
pub trait GroupRaftMessageHandler: Send + Sync {
    /// Handle vote request
    async fn handle_vote(
        &self,
        req: VoteRequest<GroupTypeConfig>,
    ) -> ConsensusResult<VoteResponse<GroupTypeConfig>>;

    /// Handle append entries request
    async fn handle_append_entries(
        &self,
        req: AppendEntriesRequest<GroupTypeConfig>,
    ) -> ConsensusResult<AppendEntriesResponse<GroupTypeConfig>>;

    /// Handle install snapshot request
    async fn handle_install_snapshot(
        &self,
        req: InstallSnapshotRequest<GroupTypeConfig>,
    ) -> ConsensusResult<InstallSnapshotResponse<GroupTypeConfig>>;

    /// Initialize cluster with members
    async fn initialize_cluster(
        &self,
        members: std::collections::BTreeMap<NodeId, proven_topology::Node>,
    ) -> ConsensusResult<()>;
}

// Declare Raft types for group consensus
openraft::declare_raft_types!(
    /// Type configuration for group consensus
    pub GroupTypeConfig:
        D = GroupRequest,
        R = GroupResponse,
        NodeId = NodeId,
        Node = proven_topology::Node,
        Entry = Entry<GroupTypeConfig>,
        SnapshotData = GroupSnapshot,
        AsyncRuntime = openraft::TokioRuntime,
);

/// Group consensus layer
pub struct GroupConsensusLayer<L: LogStorage> {
    /// Node ID
    node_id: NodeId,
    /// Group ID
    group_id: ConsensusGroupId,
    /// Raft instance
    raft: Raft<GroupTypeConfig>,
    /// State machine
    state_machine: Arc<GroupStateMachine>,
    /// Whether this group needs initialization (no persisted state)
    needs_initialization: bool,
    /// Log storage marker
    _marker: PhantomData<L>,
}

impl<L: LogStorage> GroupConsensusLayer<L> {
    /// Check if this group needs initialization
    pub fn needs_initialization(&self) -> bool {
        self.needs_initialization
    }

    /// Shutdown the Raft instance
    pub async fn shutdown(&self) -> ConsensusResult<()> {
        self.raft.shutdown().await.map_err(|e| {
            Error::with_context(
                ErrorKind::Consensus,
                format!("Failed to shutdown Raft: {e}"),
            )
        })
    }

    /// Create a new group consensus layer
    pub async fn new<NF>(
        node_id: NodeId,
        group_id: ConsensusGroupId,
        config: Config,
        network_factory: NF,
        log_storage: L,
        callbacks: Arc<dyn GroupConsensusCallbacks>,
        state: GroupStateWriter,
    ) -> ConsensusResult<Self>
    where
        NF: RaftNetworkFactory<GroupTypeConfig>,
    {
        let handler = Arc::new(GroupOperationHandler::new(group_id, state.clone()));

        // Get the last committed log index before starting (from raw storage)
        let logs_namespace = StorageNamespace::new(format!("group_{}_logs", group_id.value()));
        let replay_boundary = match log_storage.bounds(&logs_namespace).await {
            Ok(Some((_, last_index))) => Some(last_index),
            Ok(None) => None,
            Err(e) => {
                return Err(Error::with_context(
                    ErrorKind::Storage,
                    format!("Failed to get storage bounds: {e}"),
                ));
            }
        };

        // Create log storage
        let log_storage = Arc::new(GroupRaftLogStorage::new(Arc::new(log_storage), group_id));

        // Create callback dispatcher and state machine
        let callback_dispatcher = Arc::new(GroupCallbackDispatcher::new(callbacks));
        let state_machine = Arc::new(GroupStateMachine::new(
            group_id,
            state.clone(),
            handler.clone(),
            callback_dispatcher,
            replay_boundary,
        ));

        let validated_config = Arc::new(
            config
                .validate()
                .map_err(|e| Error::with_context(ErrorKind::Configuration, e.to_string()))?,
        );

        // Pass separated storage and state machine to Raft
        let raft = Raft::new(
            node_id,
            validated_config,
            network_factory,
            log_storage.clone(),   // Log storage only
            state_machine.clone(), // State machine only
        )
        .await
        .map_err(|e| Error::with_context(ErrorKind::Consensus, e.to_string()))?;

        Ok(Self {
            node_id,
            group_id,
            raft,
            state_machine,
            needs_initialization: replay_boundary.is_none(),
            _marker: PhantomData,
        })
    }

    /// Get the group ID
    pub fn group_id(&self) -> ConsensusGroupId {
        self.group_id
    }

    /// Get the Raft instance
    pub fn raft(&self) -> &Raft<GroupTypeConfig> {
        &self.raft
    }

    /// Get the state machine
    pub fn state_machine(&self) -> &Arc<GroupStateMachine> {
        &self.state_machine
    }

    /// Submit a request
    pub async fn submit_request(&self, request: GroupRequest) -> ConsensusResult<GroupResponse> {
        match self.raft.client_write(request).await {
            Ok(response) => Ok(response.data),
            Err(err) => {
                if let Some(forward_to_leader) = err.forward_to_leader() {
                    return Err(Error::not_leader(
                        format!("Not the leader for group {:?}", self.group_id),
                        forward_to_leader.leader_id,
                    ));
                }

                Err(Error::with_context(ErrorKind::Consensus, err.to_string()))
            }
        }
    }

    /// Get current leader
    pub async fn current_leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }

    /// Get current members
    pub async fn current_members(&self) -> Vec<NodeId> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .collect()
    }

    /// Get current members
    pub fn get_members(&self) -> Vec<NodeId> {
        let metrics = self.raft.metrics().borrow_watched().clone();
        metrics.membership_config.membership().voter_ids().collect()
    }

    /// Get current leader
    pub fn get_leader(&self) -> Option<NodeId> {
        let metrics = self.raft.metrics().borrow_watched().clone();
        metrics.current_leader
    }

    /// Get current term
    pub fn get_current_term(&self) -> u64 {
        let metrics = self.raft.metrics().borrow_watched().clone();
        metrics.current_term
    }

    /// Start monitoring for leader changes
    pub fn start_leader_monitoring<F>(&self, group_id: ConsensusGroupId, callback: F)
    where
        F: Fn(ConsensusGroupId, Option<NodeId>, Option<NodeId>, u64) + Send + 'static,
    {
        let mut metrics_rx = self.raft.metrics();

        tokio::spawn(async move {
            let mut current_leader: Option<NodeId> = None;
            let mut current_term: u64 = 0;

            loop {
                tokio::select! {
                    Ok(_) = metrics_rx.changed() => {
                        let metrics = metrics_rx.borrow().clone();

                        // Check for leader change or term change
                        if metrics.current_leader != current_leader || metrics.current_term != current_term {
                            let old_leader = current_leader;
                            current_leader = metrics.current_leader;
                            current_term = metrics.current_term;

                            // Call the callback
                            callback(group_id, old_leader, current_leader, current_term);
                        }
                    }
                    else => {
                        tracing::debug!("Group {} leader monitoring task stopped", group_id);
                        break;
                    }
                }
            }
        });
    }

    /// Check if Raft has been initialized
    pub async fn is_initialized(&self) -> bool {
        self.raft.is_initialized().await.unwrap_or(false)
    }
}

#[async_trait::async_trait]
impl<L: LogStorage> ConsensusLayer for GroupConsensusLayer<L> {
    type Operation = GroupOperation;
    type State = GroupState;

    async fn initialize(&self) -> ConsensusResult<()> {
        // Initialization is handled by Raft
        Ok(())
    }

    async fn propose(&self, operation: Self::Operation) -> ConsensusResult<OperationId> {
        let id = operation.id.clone();

        // Submit to Raft
        self.submit_request(operation.request).await?;

        Ok(id)
    }

    async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.node_id)
    }

    async fn current_term(&self) -> Term {
        let metrics = self.raft.metrics().borrow_watched().clone();
        Term::new(metrics.current_term)
    }

    async fn current_role(&self) -> ConsensusRole {
        let metrics = self.raft.metrics().borrow_watched().clone();
        if metrics.current_leader == Some(self.node_id) {
            ConsensusRole::Leader
        } else {
            // Check if we're a voter or learner based on the state
            // TODO: Query actual membership to determine if voter or learner
            ConsensusRole::Follower
        }
    }
}

/// Implementation of GroupRaftMessageHandler for GroupConsensusLayer
#[async_trait::async_trait]
impl<L: LogStorage> GroupRaftMessageHandler for GroupConsensusLayer<L> {
    async fn handle_vote(
        &self,
        req: VoteRequest<GroupTypeConfig>,
    ) -> ConsensusResult<VoteResponse<GroupTypeConfig>> {
        self.raft
            .vote(req)
            .await
            .map_err(|e| Error::with_context(ErrorKind::Consensus, e.to_string()))
    }

    async fn handle_append_entries(
        &self,
        req: AppendEntriesRequest<GroupTypeConfig>,
    ) -> ConsensusResult<AppendEntriesResponse<GroupTypeConfig>> {
        self.raft
            .append_entries(req)
            .await
            .map_err(|e| Error::with_context(ErrorKind::Consensus, e.to_string()))
    }

    async fn handle_install_snapshot(
        &self,
        req: InstallSnapshotRequest<GroupTypeConfig>,
    ) -> ConsensusResult<InstallSnapshotResponse<GroupTypeConfig>> {
        self.raft
            .install_snapshot(req)
            .await
            .map_err(|e| Error::with_context(ErrorKind::Consensus, e.to_string()))
    }

    async fn initialize_cluster(
        &self,
        members: std::collections::BTreeMap<NodeId, proven_topology::Node>,
    ) -> ConsensusResult<()> {
        self.raft
            .initialize(members)
            .await
            .map_err(|e| Error::with_context(ErrorKind::Consensus, e.to_string()))
    }
}
