//! Group consensus Raft integration
//!
//! This module integrates the group state with OpenRaft for consensus.
//! It uses separated storage and state machine components.

use std::sync::Arc;

use openraft::{Config, Entry, Raft, RaftMetrics, RaftNetworkFactory};
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
    /// Log storage
    log_storage: Arc<GroupRaftLogStorage<L>>,
    /// State machine
    state_machine: Arc<GroupStateMachine>,
}

impl<L: LogStorage> GroupConsensusLayer<L> {
    /// Create a new group consensus layer
    pub async fn new<NF>(
        node_id: NodeId,
        group_id: ConsensusGroupId,
        config: Config,
        network_factory: NF,
        storage: L,
    ) -> ConsensusResult<Self>
    where
        NF: RaftNetworkFactory<GroupTypeConfig>,
    {
        let state = Arc::new(GroupState::new());
        let handler = Arc::new(GroupOperationHandler::new(state.clone()));

        // Create separated storage and state machine
        let log_storage = Arc::new(GroupRaftLogStorage::new(Arc::new(storage), group_id));
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

    /// Set event publisher
    pub async fn set_event_publisher(&self, publisher: EventPublisher) {
        self.state_machine
            .set_event_context(publisher, self.node_id.clone(), self.group_id)
            .await;
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
