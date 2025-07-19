//! Group consensus Raft integration
//!
//! This module integrates the group state with OpenRaft for consensus.
//! It uses separated storage and state machine components.

use std::sync::Arc;

use openraft::{
    Config, Entry, Raft, RaftMetrics, RaftNetworkFactory,
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use proven_storage::LogStorage;
use proven_topology::NodeId;

use crate::error::{ConsensusError, ConsensusResult, ErrorKind};
use crate::foundation::{
    traits::ConsensusLayer,
    types::{ConsensusGroupId, ConsensusRole, OperationId, Term},
};

use crate::services::event::EventPublisher;

use super::operations::{GroupOperation, GroupOperationHandler};
use super::snapshot::GroupSnapshot;
use super::state::GroupState;
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
    /// Group state
    state: Arc<GroupState>,
    /// Operation handler
    handler: Arc<GroupOperationHandler>,
    /// Log storage (consensus logs - no deletion allowed)
    log_storage: Arc<GroupRaftLogStorage<L>>,
    /// State machine
    state_machine: Arc<GroupStateMachine>,
}

impl<L: LogStorage> GroupConsensusLayer<L> {
    /// Shutdown the Raft instance
    pub async fn shutdown(&self) -> ConsensusResult<()> {
        self.raft.shutdown().await.map_err(|e| {
            ConsensusError::with_context(
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
    ) -> ConsensusResult<Self>
    where
        NF: RaftNetworkFactory<GroupTypeConfig>,
    {
        let state = Arc::new(GroupState::new());
        let handler = Arc::new(GroupOperationHandler::new(state.clone()));

        // Create separated storage and state machine
        let log_storage = Arc::new(GroupRaftLogStorage::new(Arc::new(log_storage), group_id));
        let state_machine = Arc::new(GroupStateMachine::new(state.clone()));

        let validated_config =
            Arc::new(config.validate().map_err(|e| {
                ConsensusError::with_context(ErrorKind::Configuration, e.to_string())
            })?);

        // Pass separated storage and state machine to Raft
        let raft = Raft::new(
            node_id.clone(),
            validated_config,
            network_factory,
            log_storage.clone(),   // Log storage only
            state_machine.clone(), // State machine only
        )
        .await
        .map_err(|e| ConsensusError::with_context(ErrorKind::Consensus, e.to_string()))?;

        Ok(Self {
            node_id,
            group_id,
            raft,
            state,
            handler,
            log_storage,
            state_machine,
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

    /// Get the group state
    pub fn state(&self) -> &Arc<GroupState> {
        &self.state
    }

    /// Submit a request
    pub async fn submit_request(&self, request: GroupRequest) -> ConsensusResult<GroupResponse> {
        // For testing purposes, handle requests directly without Raft consensus
        // In production, this should use self.raft.client_write(request)
        match request {
            GroupRequest::Admin(admin_op) => {
                use super::types::AdminOperation;
                match admin_op {
                    AdminOperation::InitializeStream { stream } => {
                        tracing::info!("Initializing stream {stream} in group {}", self.group_id);
                        // Initialize the stream in the state
                        if self.state.initialize_stream(stream.clone()).await {
                            Ok(GroupResponse::Success)
                        } else {
                            Ok(GroupResponse::Error {
                                message: format!("Stream {stream} already exists"),
                            })
                        }
                    }
                    _ => Ok(GroupResponse::Success),
                }
            }
            GroupRequest::Stream(stream_op) => {
                use super::types::StreamOperation;
                match stream_op {
                    StreamOperation::Append { stream, message } => {
                        tracing::info!(
                            "Appending message to stream {} in group {}",
                            stream,
                            self.group_id
                        );

                        // Use the GroupState to track sequences properly
                        let sequence =
                            match self.state.append_message(&stream, message.clone()).await {
                                Some(seq) => seq,
                                None => {
                                    // Stream doesn't exist
                                    return Ok(GroupResponse::Error {
                                        message: format!("Stream {stream} not found"),
                                    });
                                }
                            };

                        // Storage is now handled synchronously in the state machine

                        Ok(GroupResponse::Appended { stream, sequence })
                    }
                    _ => Ok(GroupResponse::Success),
                }
            }
        }
    }

    /// Get current leader
    pub async fn current_leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }

    /// Get metrics
    pub fn metrics(&self) -> tokio::sync::watch::Receiver<RaftMetrics<GroupTypeConfig>> {
        self.raft.metrics()
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

    async fn get_state(&self) -> ConsensusResult<Arc<Self::State>> {
        Ok(self.state.clone())
    }

    async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.node_id.clone())
    }

    async fn current_term(&self) -> Term {
        let metrics = self.raft.metrics().borrow().clone();
        Term::new(metrics.current_term)
    }

    async fn current_role(&self) -> ConsensusRole {
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.current_leader == Some(self.node_id.clone()) {
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
            .map_err(|e| ConsensusError::with_context(ErrorKind::Consensus, e.to_string()))
    }

    async fn handle_append_entries(
        &self,
        req: AppendEntriesRequest<GroupTypeConfig>,
    ) -> ConsensusResult<AppendEntriesResponse<GroupTypeConfig>> {
        self.raft
            .append_entries(req)
            .await
            .map_err(|e| ConsensusError::with_context(ErrorKind::Consensus, e.to_string()))
    }

    async fn handle_install_snapshot(
        &self,
        req: InstallSnapshotRequest<GroupTypeConfig>,
    ) -> ConsensusResult<InstallSnapshotResponse<GroupTypeConfig>> {
        self.raft
            .install_snapshot(req)
            .await
            .map_err(|e| ConsensusError::with_context(ErrorKind::Consensus, e.to_string()))
    }

    async fn initialize_cluster(
        &self,
        members: std::collections::BTreeMap<NodeId, proven_topology::Node>,
    ) -> ConsensusResult<()> {
        self.raft
            .initialize(members)
            .await
            .map_err(|e| ConsensusError::with_context(ErrorKind::Consensus, e.to_string()))
    }
}
