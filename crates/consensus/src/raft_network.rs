//! OpenRaft network implementation for consensus messaging.

use std::collections::HashMap;
use std::sync::Arc;

use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, warn};

use crate::consensus_manager::TypeConfig;
use crate::error::ConsensusError;
use crate::network::{ConsensusMessage, ConsensusNetwork};

/// Type alias for the complex network cache type
type NetworkCache<G, A> = HashMap<u64, Arc<ConsensusRaftNetwork<G, A>>>;

/// OpenRaft-specific message types that map to our network layer
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftMessage {
    /// Vote request from a candidate
    Vote(VoteRequest<TypeConfig>),
    /// Vote response to a candidate
    VoteResponse(VoteResponse<TypeConfig>),
    /// Append entries request from leader
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    /// Append entries response to leader
    AppendEntriesResponse(AppendEntriesResponse<TypeConfig>),
    /// Install snapshot request from leader
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
    /// Install snapshot response to leader
    InstallSnapshotResponse(InstallSnapshotResponse<TypeConfig>),
}

/// Network factory for creating `OpenRaft` network instances
pub struct ConsensusRaftNetworkFactory<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static + std::fmt::Debug,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Reference to our existing network infrastructure
    consensus_network: Arc<ConsensusNetwork<G, A>>,
    /// Cache of network instances for different target nodes
    networks: Arc<RwLock<NetworkCache<G, A>>>,
    /// Sender for outbound network messages
    network_tx: mpsc::UnboundedSender<(u64, ConsensusMessage)>,
}

impl<G, A> ConsensusRaftNetworkFactory<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static + std::fmt::Debug,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Create a new network factory
    #[must_use]
    pub fn new(
        consensus_network: Arc<ConsensusNetwork<G, A>>,
        network_tx: mpsc::UnboundedSender<(u64, ConsensusMessage)>,
    ) -> Self {
        Self {
            consensus_network,
            networks: Arc::new(RwLock::new(HashMap::new())),
            network_tx,
        }
    }
}

impl<G, A> RaftNetworkFactory<TypeConfig> for ConsensusRaftNetworkFactory<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static + std::fmt::Debug,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    type Network = ConsensusRaftNetwork<G, A>;

    /// Create or get a network instance for communicating with a specific target node
    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        // Check if we already have a network instance for this target
        {
            let networks = self.networks.read().await;
            if let Some(network) = networks.get(&target) {
                return (**network).clone();
            }
        }

        // Create a new network instance for this target
        let network = ConsensusRaftNetwork::new(
            self.consensus_network.clone(),
            target,
            self.network_tx.clone(),
        );

        // Cache it for future use
        {
            let mut networks = self.networks.write().await;
            networks.insert(target, Arc::new(network.clone()));
        }

        network
    }
}

/// `OpenRaft` network implementation for a specific target node
#[derive(Clone)]
pub struct ConsensusRaftNetwork<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static + std::fmt::Debug,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Reference to our consensus network
    #[allow(dead_code)]
    consensus_network: Arc<ConsensusNetwork<G, A>>,
    /// Target node ID for this network instance
    target_node_id: u64,
    /// Sender for outbound network messages
    network_tx: mpsc::UnboundedSender<(u64, ConsensusMessage)>,
}

impl<G, A> ConsensusRaftNetwork<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static + std::fmt::Debug,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Create a new network instance for a specific target
    #[must_use]
    pub const fn new(
        consensus_network: Arc<ConsensusNetwork<G, A>>,
        target_node_id: u64,
        network_tx: mpsc::UnboundedSender<(u64, ConsensusMessage)>,
    ) -> Self {
        Self {
            consensus_network,
            target_node_id,
            network_tx,
        }
    }

    /// Send a raft message to the target node
    #[allow(clippy::result_large_err)]
    fn send_raft_message(
        &self,
        message: RaftMessage,
    ) -> Result<RaftMessage, RPCError<TypeConfig, RaftError<TypeConfig>>> {
        debug!(
            "Sending Raft message to node {}: {:?}",
            self.target_node_id, message
        );

        // Convert RaftMessage to our network's ConsensusMessage
        let consensus_msg = match message {
            RaftMessage::Vote(_) => ConsensusMessage::Vote,
            RaftMessage::AppendEntries(_) => ConsensusMessage::AppendEntries,
            RaftMessage::InstallSnapshot(_) => ConsensusMessage::InstallSnapshot,
            _ => ConsensusMessage::Data(
                serde_json::to_vec(&message)
                    .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?
                    .into(),
            ),
        };

        // Send the message through our consensus network
        if let Err(e) = self.network_tx.send((self.target_node_id, consensus_msg)) {
            let error_msg = format!(
                "Failed to send message to node {}: {}",
                self.target_node_id, e
            );
            warn!("{}", error_msg);
            return Err(RPCError::Network(openraft::error::NetworkError::new(
                &ConsensusError::Network(error_msg.into()),
            )));
        }

        debug!(
            "Successfully queued Raft message for node {}",
            self.target_node_id
        );

        // For now, we'll simulate responses since this is a consensus protocol
        // In a real implementation, responses would come through the consensus_rx channel
        // and be handled by the consensus protocol state machine
        match message {
            RaftMessage::Vote(req) => {
                let response = VoteResponse::new(
                    req.vote,
                    req.last_log_id,
                    false, // For now, always reject votes - this will be handled properly by Raft consensus
                );
                Ok(RaftMessage::VoteResponse(response))
            }
            RaftMessage::AppendEntries(_req) => {
                // Create a failure response for append entries
                let response = AppendEntriesResponse::Conflict;
                Ok(RaftMessage::AppendEntriesResponse(response))
            }
            RaftMessage::InstallSnapshot(req) => {
                let response = InstallSnapshotResponse { vote: req.vote };
                Ok(RaftMessage::InstallSnapshotResponse(response))
            }
            _ => Err(RPCError::Network(openraft::error::NetworkError::new(
                &ConsensusError::InvalidMessage("Unexpected message type".to_string()),
            ))),
        }
    }
}

impl<G, A> RaftNetwork<TypeConfig> for ConsensusRaftNetwork<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static + std::fmt::Debug,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Send a vote request to the target node
    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>> {
        debug!("Sending vote request to node {}", self.target_node_id);

        let message = RaftMessage::Vote(rpc);
        match self.send_raft_message(message)? {
            RaftMessage::VoteResponse(response) => Ok(response),
            _ => Err(RPCError::Network(openraft::error::NetworkError::new(
                &ConsensusError::InvalidMessage("Invalid vote response".to_string()),
            ))),
        }
    }

    /// Send an append entries request to the target node
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>>
    {
        debug!(
            "Sending append entries request to node {}",
            self.target_node_id
        );

        let message = RaftMessage::AppendEntries(rpc);
        match self.send_raft_message(message)? {
            RaftMessage::AppendEntriesResponse(response) => Ok(response),
            _ => Err(RPCError::Network(openraft::error::NetworkError::new(
                &ConsensusError::InvalidMessage("Invalid append entries response".to_string()),
            ))),
        }
    }

    /// Send an install snapshot request to the target node
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<TypeConfig>,
        RPCError<TypeConfig, RaftError<TypeConfig, InstallSnapshotError>>,
    > {
        debug!(
            "Sending install snapshot request to node {}",
            self.target_node_id
        );

        // Handle install snapshot directly to avoid error type conversion issues
        warn!(
            "Simulating install snapshot response for node {} (network integration pending)",
            self.target_node_id
        );

        let response = InstallSnapshotResponse { vote: rpc.vote };
        Ok(response)
    }
}

impl<G, A> std::fmt::Debug for ConsensusRaftNetwork<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static + std::fmt::Debug,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsensusRaftNetwork")
            .field("target_node_id", &self.target_node_id)
            .finish_non_exhaustive()
    }
}
