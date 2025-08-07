//! Global consensus Raft integration
//!
//! This module integrates the global state with OpenRaft for consensus.
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
    GlobalState, GlobalStateWriter,
    traits::ConsensusLayer,
    types::{ConsensusRole, OperationId, Term},
};

/// Trait for handling Raft RPC messages
#[async_trait::async_trait]
pub trait GlobalRaftMessageHandler: Send + Sync {
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
        members: std::collections::BTreeMap<NodeId, proven_topology::Node>,
    ) -> ConsensusResult<()>;

    /// Change membership
    async fn change_membership(
        &self,
        members: std::collections::BTreeMap<NodeId, proven_topology::Node>,
        retain: bool,
    ) -> ConsensusResult<()>;
}

use super::callbacks::GlobalConsensusCallbacks;
use super::dispatcher::GlobalCallbackDispatcher;
use super::operations::{GlobalOperation, GlobalOperationHandler};
use super::snapshot::GlobalSnapshot;
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
        Node = proven_topology::Node,
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
    /// State machine
    state_machine: Arc<GlobalStateMachine>,

    _marker: PhantomData<L>,
}

#[async_trait::async_trait]
impl<L: LogStorage> GlobalRaftMessageHandler for GlobalConsensusLayer<L> {
    async fn handle_vote(
        &self,
        req: VoteRequest<GlobalTypeConfig>,
    ) -> ConsensusResult<VoteResponse<GlobalTypeConfig>> {
        self.raft
            .vote(req)
            .await
            .map_err(|e| Error::with_context(ErrorKind::Consensus, format!("Vote failed: {e}")))
    }

    async fn handle_append_entries(
        &self,
        req: AppendEntriesRequest<GlobalTypeConfig>,
    ) -> ConsensusResult<AppendEntriesResponse<GlobalTypeConfig>> {
        self.raft.append_entries(req).await.map_err(|e| {
            Error::with_context(ErrorKind::Consensus, format!("Append entries failed: {e}"))
        })
    }

    async fn handle_install_snapshot(
        &self,
        req: InstallSnapshotRequest<GlobalTypeConfig>,
    ) -> ConsensusResult<InstallSnapshotResponse<GlobalTypeConfig>> {
        self.raft.install_snapshot(req).await.map_err(|e| {
            Error::with_context(
                ErrorKind::Consensus,
                format!("Install snapshot failed: {e}"),
            )
        })
    }

    async fn initialize_cluster(
        &self,
        members: std::collections::BTreeMap<NodeId, proven_topology::Node>,
    ) -> ConsensusResult<()> {
        self.raft.initialize(members).await.map_err(|e| {
            Error::with_context(
                ErrorKind::Consensus,
                format!("Failed to initialize Raft: {e}"),
            )
        })
    }

    async fn change_membership(
        &self,
        members: std::collections::BTreeMap<NodeId, proven_topology::Node>,
        retain: bool,
    ) -> ConsensusResult<()> {
        // Convert BTreeMap to BTreeSet for the membership change
        let member_ids: std::collections::BTreeSet<NodeId> = members.keys().cloned().collect();
        self.change_membership(member_ids, retain).await
    }
}

impl<L: LogStorage> GlobalConsensusLayer<L> {
    /// Shutdown the Raft instance
    pub async fn shutdown(&self) -> ConsensusResult<()> {
        self.raft.shutdown().await.map_err(|e| {
            Error::with_context(
                ErrorKind::Consensus,
                format!("Failed to shutdown Raft: {e}"),
            )
        })
    }

    /// Add a node as a learner (non-voting member)
    pub async fn add_learner(
        &self,
        node_id: NodeId,
        node_info: proven_topology::Node,
    ) -> ConsensusResult<()> {
        self.raft
            .add_learner(node_id, node_info, true)
            .await
            .map_err(|e| {
                Error::with_context(ErrorKind::Consensus, format!("Failed to add learner: {e}"))
            })?;
        Ok(())
    }

    /// Add a node to the consensus cluster
    pub async fn add_node(
        &self,
        node_id: NodeId,
        node_info: proven_topology::Node,
    ) -> ConsensusResult<()> {
        // Get current membership
        let metrics = self.raft.metrics().borrow_watched().clone();
        let current_membership = &metrics.membership_config;
        let current_members: Vec<NodeId> = current_membership.membership().voter_ids().collect();

        // Check if node is already a member
        if current_members.contains(&node_id) {
            return Ok(());
        }

        // Add as learner first
        self.add_learner(node_id.clone(), node_info).await?;

        // Give learner time to catch up
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Build new membership including the new node
        let mut new_members = std::collections::BTreeSet::new();
        for member in current_members {
            new_members.insert(member);
        }
        new_members.insert(node_id);

        // Promote to voter
        self.change_membership(new_members, false).await?;

        Ok(())
    }

    /// Remove a node from the consensus cluster
    pub async fn remove_node(&self, node_id: NodeId) -> ConsensusResult<()> {
        // Get current membership
        let metrics = self.raft.metrics().borrow_watched().clone();
        let current_membership = &metrics.membership_config;
        let current_members: Vec<NodeId> = current_membership.membership().voter_ids().collect();

        // Check if node is not a member
        if !current_members.contains(&node_id) {
            return Ok(());
        }

        // Build new membership without the removed node
        let mut new_members = std::collections::BTreeSet::new();
        for member in current_members {
            if member != node_id {
                new_members.insert(member);
            }
        }

        // Update membership
        self.change_membership(new_members, false).await?;

        Ok(())
    }

    /// Change membership (promote learners to voters or remove members)
    pub async fn change_membership(
        &self,
        members: std::collections::BTreeSet<NodeId>,
        retain: bool,
    ) -> ConsensusResult<()> {
        self.raft
            .change_membership(members, retain)
            .await
            .map_err(|e| {
                Error::with_context(
                    ErrorKind::Consensus,
                    format!("Failed to change membership: {e}"),
                )
            })?;
        Ok(())
    }

    /// Check if this node is the current leader
    pub async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.node_id.clone())
    }

    /// Check if Raft has been initialized
    pub async fn is_initialized(&self) -> bool {
        self.raft.is_initialized().await.unwrap_or(false)
    }

    /// Create a new global consensus layer
    pub async fn new<NF>(
        node_id: NodeId,
        config: Config,
        network_factory: NF,
        storage: L,
        callbacks: Arc<dyn GlobalConsensusCallbacks>,
        state: GlobalStateWriter,
    ) -> ConsensusResult<Self>
    where
        NF: RaftNetworkFactory<GlobalTypeConfig>,
    {
        let handler = Arc::new(GlobalOperationHandler::new(state.clone()));

        // Get the last committed log index before starting (from raw storage)
        let logs_namespace = StorageNamespace::new("global_logs");
        let replay_boundary = match storage.bounds(&logs_namespace).await {
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
        let log_storage = Arc::new(GlobalRaftLogStorage::new(Arc::new(storage)));

        // Create callback dispatcher and state machine
        let callback_dispatcher = Arc::new(GlobalCallbackDispatcher::new(callbacks));
        let state_machine = Arc::new(GlobalStateMachine::new(
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
            node_id.clone(),
            validated_config,
            network_factory,
            log_storage.clone(),   // Log storage only
            state_machine.clone(), // State machine only
        )
        .await
        .map_err(|e| Error::with_context(ErrorKind::Consensus, e.to_string()))?;

        Ok(Self {
            node_id,
            raft,
            state_machine,
            _marker: PhantomData,
        })
    }

    /// Get the state machine
    pub fn state_machine(&self) -> &Arc<GlobalStateMachine> {
        &self.state_machine
    }

    /// Submit a request
    pub async fn submit_request(&self, request: GlobalRequest) -> ConsensusResult<GlobalResponse> {
        match self.raft.client_write(request).await {
            Ok(response) => Ok(response.data),
            Err(err) => {
                if let Some(forward_to_leader) = err.forward_to_leader() {
                    return Err(Error::not_leader(
                        "Not the leader for global consensus",
                        forward_to_leader.leader_id.clone(),
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
    pub fn start_leader_monitoring(&self, callbacks: Arc<dyn GlobalConsensusCallbacks>) {
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
                            let old_leader = current_leader.clone();
                            current_leader = metrics.current_leader.clone();
                            current_term = metrics.current_term;

                            // Call the callback with proper term
                            if let Err(e) = callbacks.on_leader_changed(
                                old_leader,
                                current_leader.clone(),
                                current_term
                            ).await {
                                tracing::error!("Failed to handle leader change: {}", e);
                            }
                        }
                    }
                    else => {
                        tracing::debug!("Leader monitoring task stopped");
                        break;
                    }
                }
            }
        });
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

    async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.node_id.clone())
    }

    async fn current_term(&self) -> Term {
        let metrics = self.raft.metrics().borrow_watched().clone();
        Term::new(metrics.current_term)
    }

    async fn current_role(&self) -> ConsensusRole {
        let metrics = self.raft.metrics().borrow_watched().clone();
        if metrics.current_leader == Some(self.node_id.clone()) {
            ConsensusRole::Leader
        } else {
            // Check if we're a voter or learner based on the state
            // TODO: Query actual membership to determine if voter or learner
            ConsensusRole::Follower
        }
    }
}
