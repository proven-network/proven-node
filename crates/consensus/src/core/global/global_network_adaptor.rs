//! Network adapter for global consensus
//!
//! This module provides the network adapter implementation that bridges
//! OpenRaft with our transport layer for global consensus.

use std::marker::PhantomData;
use std::sync::Arc;

use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::core::global::GlobalConsensusTypeConfig;
use crate::network::messages::{GlobalMessage, GlobalRaftMessage, Message};
use proven_governance::Governance;
use proven_network::NetworkManager;
use proven_topology::NodeId;
use proven_transport::Transport;

/// Factory for creating global Raft network instances
pub struct GlobalNetworkFactory<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Network manager for sending messages
    network_manager: Arc<NetworkManager<T, G>>,
    /// Marker for the type config
    _marker: PhantomData<GlobalConsensusTypeConfig>,
}

impl<T, G> GlobalNetworkFactory<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new global Raft network factory
    pub fn new(network_manager: Arc<NetworkManager<T, G>>) -> Self {
        Self {
            network_manager,
            _marker: PhantomData,
        }
    }
}

impl<T, G> RaftNetworkFactory<GlobalConsensusTypeConfig> for GlobalNetworkFactory<T, G>
where
    T: Transport,
    G: Governance,
{
    type Network = GlobalRaftAdapter<T, G>;

    async fn new_client(&mut self, target: NodeId, _node: &proven_topology::Node) -> Self::Network {
        GlobalRaftAdapter {
            network_manager: self.network_manager.clone(),
            target_node_id: target,
        }
    }
}

/// Network adapter for global consensus
pub struct GlobalRaftAdapter<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Network manager for sending messages
    network_manager: Arc<NetworkManager<T, G>>,
    /// Target node ID
    target_node_id: NodeId,
}

impl<T, G> RaftNetwork<GlobalConsensusTypeConfig> for GlobalRaftAdapter<T, G>
where
    T: Transport,
    G: Governance,
{
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<GlobalConsensusTypeConfig>,
        option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<GlobalConsensusTypeConfig>,
        RPCError<GlobalConsensusTypeConfig, RaftError<GlobalConsensusTypeConfig>>,
    > {
        let timeout = option.hard_ttl();

        // Create the network message
        let message = Message::Global(Box::new(GlobalMessage::Raft(
            GlobalRaftMessage::AppendEntriesRequest(rpc),
        )));

        // Get peer and send request
        let peer = self
            .network_manager
            .get_peer(&self.target_node_id)
            .await
            .map_err(|_| {
                RPCError::Network(openraft::error::NetworkError::new(&std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Peer {} not found", self.target_node_id),
                )))
            })?;

        // Send request and wait for response directly
        match peer.request(message, timeout).await {
            Ok(response_msg) => {
                // Extract the append entries response
                if let Message::Global(global_msg) = response_msg
                    && let GlobalMessage::Raft(GlobalRaftMessage::AppendEntriesResponse(resp)) =
                        *global_msg
                {
                    return Ok(resp);
                }

                Err(RPCError::Network(openraft::error::NetworkError::new(
                    &std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Unexpected response type",
                    ),
                )))
            }
            Err(e) => Err(RPCError::Network(openraft::error::NetworkError::new(&e))),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<GlobalConsensusTypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<GlobalConsensusTypeConfig>,
        RPCError<
            GlobalConsensusTypeConfig,
            RaftError<GlobalConsensusTypeConfig, InstallSnapshotError>,
        >,
    > {
        let timeout = option.hard_ttl();

        // Create the network message
        let message = Message::Global(Box::new(GlobalMessage::Raft(
            GlobalRaftMessage::InstallSnapshotRequest(rpc),
        )));

        // Get peer and send request
        let peer = self
            .network_manager
            .get_peer(&self.target_node_id)
            .await
            .map_err(|_| {
                RPCError::Network(openraft::error::NetworkError::new(&std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Peer {} not found", self.target_node_id),
                )))
            })?;

        // Send request and wait for response directly
        match peer.request(message, timeout).await {
            Ok(response_msg) => {
                // Extract the install snapshot response
                if let Message::Global(global_msg) = response_msg
                    && let GlobalMessage::Raft(GlobalRaftMessage::InstallSnapshotResponse(resp)) =
                        *global_msg
                {
                    return Ok(resp);
                }

                Err(RPCError::Network(openraft::error::NetworkError::new(
                    &std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Unexpected response type",
                    ),
                )))
            }
            Err(e) => Err(RPCError::Network(openraft::error::NetworkError::new(&e))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<GlobalConsensusTypeConfig>,
        option: RPCOption,
    ) -> Result<
        VoteResponse<GlobalConsensusTypeConfig>,
        RPCError<GlobalConsensusTypeConfig, RaftError<GlobalConsensusTypeConfig>>,
    > {
        let timeout = option.hard_ttl();

        // Create the network message
        let message = Message::Global(Box::new(GlobalMessage::Raft(
            GlobalRaftMessage::VoteRequest(rpc),
        )));

        // Get peer and send request
        let peer = self
            .network_manager
            .get_peer(&self.target_node_id)
            .await
            .map_err(|_| {
                RPCError::Network(openraft::error::NetworkError::new(&std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Peer {} not found", self.target_node_id),
                )))
            })?;

        // Send request and wait for response directly
        match peer.request(message, timeout).await {
            Ok(response_msg) => {
                // Extract the vote response
                if let Message::Global(global_msg) = response_msg
                    && let GlobalMessage::Raft(GlobalRaftMessage::VoteResponse(resp)) = *global_msg
                {
                    return Ok(resp);
                }

                Err(RPCError::Network(openraft::error::NetworkError::new(
                    &std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Unexpected response type",
                    ),
                )))
            }
            Err(e) => Err(RPCError::Network(openraft::error::NetworkError::new(&e))),
        }
    }
}
