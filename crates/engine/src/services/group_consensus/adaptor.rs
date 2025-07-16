//! Network adaptor for group consensus Raft integration

use std::sync::Arc;

use openraft::{
    error::{InstallSnapshotError, RPCError, RaftError},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use tokio::sync::RwLock;

use proven_network::NetworkManager;
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use super::messages::*;
use crate::consensus::group::GroupTypeConfig;
use crate::foundation::types::ConsensusGroupId;
use crate::services::network::NetworkStats;

/// Factory for creating group Raft network instances
pub struct GroupNetworkFactory<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    network_manager: Arc<NetworkManager<T, G>>,
    group_id: ConsensusGroupId,
    stats: Arc<RwLock<NetworkStats>>,
}

impl<T, G> GroupNetworkFactory<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Create a new group network factory
    pub fn new(
        network_manager: Arc<NetworkManager<T, G>>,
        group_id: ConsensusGroupId,
        stats: Arc<RwLock<NetworkStats>>,
    ) -> Self {
        Self {
            network_manager,
            group_id,
            stats,
        }
    }
}

impl<T, G> RaftNetworkFactory<GroupTypeConfig> for GroupNetworkFactory<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    type Network = GroupRaftNetworkAdapter<T, G>;

    async fn new_client(&mut self, target: NodeId, _node: &proven_topology::Node) -> Self::Network {
        GroupRaftNetworkAdapter {
            network_manager: self.network_manager.clone(),
            group_id: self.group_id,
            stats: self.stats.clone(),
            target_node_id: target,
        }
    }
}

/// Network adapter for group consensus
pub struct GroupRaftNetworkAdapter<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Network manager for sending messages
    network_manager: Arc<NetworkManager<T, G>>,
    /// Group ID
    group_id: ConsensusGroupId,
    /// Network statistics
    stats: Arc<RwLock<NetworkStats>>,
    /// Target node ID
    target_node_id: NodeId,
}

impl<T, G> RaftNetwork<GroupTypeConfig> for GroupRaftNetworkAdapter<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<GroupTypeConfig>,
        option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<GroupTypeConfig>,
        RPCError<GroupTypeConfig, RaftError<GroupTypeConfig>>,
    > {
        let timeout = option.hard_ttl();

        // Create the request message
        let message = GroupAppendEntriesRequest {
            group_id: self.group_id,
            request: rpc,
        };

        // Send request and wait for response
        let response: GroupAppendEntriesResponse = self
            .network_manager
            .request_namespaced(
                super::messages::GROUP_CONSENSUS_NAMESPACE,
                self.target_node_id.clone(),
                message,
                timeout,
            )
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.messages_sent += 1;
            stats.messages_received += 1;
        }

        // Verify group ID and extract the inner response
        if response.group_id == self.group_id {
            Ok(response.response)
        } else {
            Err(RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::InvalidData, "Group ID mismatch"),
            )))
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<GroupTypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<GroupTypeConfig>,
        RPCError<GroupTypeConfig, RaftError<GroupTypeConfig, InstallSnapshotError>>,
    > {
        let timeout = option.hard_ttl();

        // Create the request message
        let message = GroupInstallSnapshotRequest {
            group_id: self.group_id,
            request: rpc,
        };

        // Send request and wait for response
        let response: GroupInstallSnapshotResponse = self
            .network_manager
            .request_namespaced(
                super::messages::GROUP_CONSENSUS_NAMESPACE,
                self.target_node_id.clone(),
                message,
                timeout,
            )
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.messages_sent += 1;
            stats.messages_received += 1;
        }

        // Verify group ID and extract the inner response
        if response.group_id == self.group_id {
            Ok(response.response)
        } else {
            Err(RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::InvalidData, "Group ID mismatch"),
            )))
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<GroupTypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<GroupTypeConfig>, RPCError<GroupTypeConfig, RaftError<GroupTypeConfig>>>
    {
        let timeout = option.hard_ttl();

        // Create the request message
        let message = GroupVoteRequest {
            group_id: self.group_id,
            request: rpc,
        };

        // Send request and wait for response
        let response: GroupVoteResponse = self
            .network_manager
            .request_namespaced(
                super::messages::GROUP_CONSENSUS_NAMESPACE,
                self.target_node_id.clone(),
                message,
                timeout,
            )
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.messages_sent += 1;
            stats.messages_received += 1;
        }

        // Verify group ID and extract the inner response
        if response.group_id == self.group_id {
            Ok(response.response)
        } else {
            Err(RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::InvalidData, "Group ID mismatch"),
            )))
        }
    }
}
