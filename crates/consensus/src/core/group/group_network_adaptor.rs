//! Network adapter for group consensus
//!
//! This module provides the network adapter implementation that bridges
//! OpenRaft with our transport layer for group consensus.

use std::marker::PhantomData;
use std::sync::Arc;

use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::ConsensusGroupId;
use crate::core::group::GroupConsensusTypeConfig;
use crate::network::messages::{GroupMessage, GroupRaftMessage, Message};
use proven_governance::Governance;
use proven_network::NetworkManager;
use proven_topology::NodeId;
use proven_transport::Transport;

/// Factory for creating group Raft network instances
pub struct GroupNetworkFactory<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Network manager for sending messages
    network_manager: Arc<NetworkManager<T, G>>,
    /// Group ID
    group_id: ConsensusGroupId,
    /// Marker for the type config
    _marker: PhantomData<GroupConsensusTypeConfig>,
}

impl<T, G> GroupNetworkFactory<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new group Raft network factory
    pub fn new(network_manager: Arc<NetworkManager<T, G>>, group_id: ConsensusGroupId) -> Self {
        Self {
            network_manager,
            group_id,
            _marker: PhantomData,
        }
    }
}

impl<T, G> RaftNetworkFactory<GroupConsensusTypeConfig> for GroupNetworkFactory<T, G>
where
    T: Transport,
    G: Governance,
{
    type Network = GroupRaftAdapter<T, G>;

    async fn new_client(&mut self, target: NodeId, _node: &proven_topology::Node) -> Self::Network {
        GroupRaftAdapter {
            network_manager: self.network_manager.clone(),
            target_node_id: target,
            group_id: self.group_id,
        }
    }
}

/// Network adapter for group consensus
pub struct GroupRaftAdapter<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Network manager for sending messages
    network_manager: Arc<NetworkManager<T, G>>,
    /// Target node ID
    target_node_id: NodeId,
    /// Group ID
    group_id: ConsensusGroupId,
}

impl<T, G> RaftNetwork<GroupConsensusTypeConfig> for GroupRaftAdapter<T, G>
where
    T: Transport,
    G: Governance,
{
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<GroupConsensusTypeConfig>,
        option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<GroupConsensusTypeConfig>,
        RPCError<GroupConsensusTypeConfig, RaftError<GroupConsensusTypeConfig>>,
    > {
        let timeout = option.hard_ttl();

        // Create the network message
        let message = Message::Local(Box::new(GroupMessage::Raft {
            group_id: self.group_id,
            message: GroupRaftMessage::AppendEntriesRequest(rpc),
        }));

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

        // Send request and wait for response
        match peer.request(message, timeout).await {
            Ok(response_msg) => {
                // Extract the append entries response
                if let Message::Local(local_msg) = response_msg
                    && let GroupMessage::Raft {
                        group_id: _,
                        message,
                    } = *local_msg
                    && let GroupRaftMessage::AppendEntriesResponse(resp) = message
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
        rpc: InstallSnapshotRequest<GroupConsensusTypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<GroupConsensusTypeConfig>,
        RPCError<
            GroupConsensusTypeConfig,
            RaftError<GroupConsensusTypeConfig, InstallSnapshotError>,
        >,
    > {
        let timeout = option.hard_ttl();

        // Create the network message
        let message = Message::Local(Box::new(GroupMessage::Raft {
            group_id: self.group_id,
            message: GroupRaftMessage::InstallSnapshotRequest(rpc),
        }));

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

        // Send request and wait for response
        match peer.request(message, timeout).await {
            Ok(response_msg) => {
                // Extract the install snapshot response
                if let Message::Local(local_msg) = response_msg
                    && let GroupMessage::Raft {
                        group_id: _,
                        message,
                    } = *local_msg
                    && let GroupRaftMessage::InstallSnapshotResponse(resp) = message
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
        rpc: VoteRequest<GroupConsensusTypeConfig>,
        option: RPCOption,
    ) -> Result<
        VoteResponse<GroupConsensusTypeConfig>,
        RPCError<GroupConsensusTypeConfig, RaftError<GroupConsensusTypeConfig>>,
    > {
        let timeout = option.hard_ttl();

        // Create the network message
        let message = Message::Local(Box::new(GroupMessage::Raft {
            group_id: self.group_id,
            message: GroupRaftMessage::VoteRequest(rpc),
        }));

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

        // Send request and wait for response
        match peer.request(message, timeout).await {
            Ok(response_msg) => {
                // Extract the vote response
                if let Message::Local(local_msg) = response_msg
                    && let GroupMessage::Raft {
                        group_id: _,
                        message,
                    } = *local_msg
                    && let GroupRaftMessage::VoteResponse(resp) = message
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
