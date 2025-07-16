//! Global consensus Raft integration
//!
//! This module integrates the global state with OpenRaft for consensus.
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
    types::{ConsensusRole, OperationId, Term},
};
use crate::services::event::EventPublisher;

/// Trait for handling Raft RPC messages
#[async_trait::async_trait]
pub trait RaftMessageHandler: Send + Sync {
    /// Handle vote request
    async fn handle_vote(
        &self,
        req: VoteRequest<GlobalTypeConfig>,
    ) -> ConsensusResult<VoteResponse<GlobalTypeConfig>>;

    /// Handle append entries request
    async fn handle_append_entries(
        &self,
        req: AppendEntriesRequest<GlobalTypeConfig>,
    ) -> ConsensusResult<AppendEntriesResponse<GlobalTypeConfig>>;

    /// Handle install snapshot request
    async fn handle_install_snapshot(
        &self,
        req: InstallSnapshotRequest<GlobalTypeConfig>,
    ) -> ConsensusResult<InstallSnapshotResponse<GlobalTypeConfig>>;

    /// Initialize cluster with members
    async fn initialize_cluster(
        &self,
        members: std::collections::BTreeMap<NodeId, proven_governance::GovernanceNode>,
    ) -> ConsensusResult<()>;
}

use super::operations::{GlobalOperation, GlobalOperationHandler};
use super::snapshot::GlobalSnapshot;
use super::state::GlobalState;
use super::state_machine::GlobalStateMachine;
use super::storage::GlobalRaftLogStorage;
use super::types::{GlobalRequest, GlobalResponse};

// Declare Raft types for global consensus
openraft::declare_raft_types!(
    /// Type configuration for global consensus
    pub GlobalTypeConfig:
        D = GlobalRequest,
        R = GlobalResponse,
        NodeId = NodeId,
        Node = proven_governance::GovernanceNode,
        Entry = Entry<GlobalTypeConfig>,
        SnapshotData = GlobalSnapshot,
        AsyncRuntime = openraft::TokioRuntime,
);

/// Global consensus layer
pub struct GlobalConsensusLayer<L: LogStorage> {
    /// Node ID
    node_id: NodeId,
    /// Raft instance
    raft: Raft<GlobalTypeConfig>,
    /// Global state
    state: Arc<GlobalState>,
    /// Operation handler
    handler: Arc<GlobalOperationHandler>,
    /// Log storage
    log_storage: Arc<GlobalRaftLogStorage<L>>,
    /// State machine
    state_machine: Arc<GlobalStateMachine>,
    /// Event publisher
    event_publisher: Option<EventPublisher>,
}

#[async_trait::async_trait]
impl<L: LogStorage> RaftMessageHandler for GlobalConsensusLayer<L> {
    async fn handle_vote(
        &self,
        req: VoteRequest<GlobalTypeConfig>,
    ) -> ConsensusResult<VoteResponse<GlobalTypeConfig>> {
        self.raft.vote(req).await.map_err(|e| {
            ConsensusError::with_context(ErrorKind::Consensus, format!("Vote failed: {e}"))
        })
    }

    async fn handle_append_entries(
        &self,
        req: AppendEntriesRequest<GlobalTypeConfig>,
    ) -> ConsensusResult<AppendEntriesResponse<GlobalTypeConfig>> {
        self.raft.append_entries(req).await.map_err(|e| {
            ConsensusError::with_context(
                ErrorKind::Consensus,
                format!("Append entries failed: {e}"),
            )
        })
    }

    async fn handle_install_snapshot(
        &self,
        req: InstallSnapshotRequest<GlobalTypeConfig>,
    ) -> ConsensusResult<InstallSnapshotResponse<GlobalTypeConfig>> {
        self.raft.install_snapshot(req).await.map_err(|e| {
            ConsensusError::with_context(
                ErrorKind::Consensus,
                format!("Install snapshot failed: {e}"),
            )
        })
    }

    async fn initialize_cluster(
        &self,
        members: std::collections::BTreeMap<NodeId, proven_governance::GovernanceNode>,
    ) -> ConsensusResult<()> {
        self.raft.initialize(members).await.map_err(|e| {
            ConsensusError::with_context(
                ErrorKind::Consensus,
                format!("Failed to initialize Raft: {e}"),
            )
        })
    }
}

impl<L: LogStorage> GlobalConsensusLayer<L> {
    /// Create a new global consensus layer
    pub async fn new<NF>(
        node_id: NodeId,
        config: Config,
        network_factory: NF,
        storage: L,
    ) -> ConsensusResult<Self>
    where
        NF: RaftNetworkFactory<GlobalTypeConfig>,
    {
        let state = Arc::new(GlobalState::new());
        let handler = Arc::new(GlobalOperationHandler::new(state.clone()));

        // Create separated storage and state machine
        let log_storage = Arc::new(GlobalRaftLogStorage::new(Arc::new(storage)));
        let state_machine = Arc::new(GlobalStateMachine::new(state.clone()));

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
            raft,
            state,
            handler,
            log_storage,
            state_machine,
            event_publisher: None,
        })
    }

    /// Set event publisher
    pub async fn set_event_publisher(&mut self, publisher: EventPublisher) {
        self.event_publisher = Some(publisher.clone());
        // Also set it on the state machine
        self.state_machine
            .set_event_publisher(publisher, self.node_id.clone())
            .await;
    }

    /// Get the global state
    pub fn state(&self) -> &Arc<GlobalState> {
        &self.state
    }

    /// Submit a request
    pub async fn submit_request(&self, request: GlobalRequest) -> ConsensusResult<GlobalResponse> {
        let response = self
            .raft
            .client_write(request)
            .await
            .map_err(|e| ConsensusError::with_context(ErrorKind::Consensus, e.to_string()))?
            .data;

        Ok(response)
    }

    /// Get current leader
    pub async fn current_leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }

    /// Get metrics
    pub fn metrics(&self) -> tokio::sync::watch::Receiver<RaftMetrics<GlobalTypeConfig>> {
        self.raft.metrics()
    }
}

#[async_trait::async_trait]
impl<L: LogStorage> ConsensusLayer for GlobalConsensusLayer<L> {
    type Operation = GlobalOperation;
    type State = GlobalState;

    async fn initialize(&self) -> ConsensusResult<()> {
        // This is called when the consensus layer is ready to start processing
        // The actual Raft initialization (with members) happens via initialize_cluster
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
