//! Network adaptor for global consensus Raft integration

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

use proven_governance::Governance;
use proven_network::NetworkManager;
use proven_topology::NodeId;
use proven_transport::Transport;

use super::messages::*;
use crate::consensus::global::GlobalTypeConfig;
use crate::services::network::NetworkStats;

/// Factory for creating global Raft network instances
pub struct GlobalNetworkFactory<T, G>
where
    T: Transport,
    G: Governance,
{
    network_manager: Arc<NetworkManager<T, G>>,
    stats: Arc<RwLock<NetworkStats>>,
}

impl<T, G> GlobalNetworkFactory<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new global network factory
    pub fn new(
        network_manager: Arc<NetworkManager<T, G>>,
        stats: Arc<RwLock<NetworkStats>>,
    ) -> Self {
        Self {
            network_manager,
            stats,
        }
    }
}

impl<T, G> RaftNetworkFactory<GlobalTypeConfig> for GlobalNetworkFactory<T, G>
where
    T: Transport,
    G: Governance,
{
    type Network = GlobalRaftNetworkAdapter<T, G>;

    async fn new_client(
        &mut self,
        target: NodeId,
        _node: &proven_governance::GovernanceNode,
    ) -> Self::Network {
        GlobalRaftNetworkAdapter {
            network_manager: self.network_manager.clone(),
            stats: self.stats.clone(),
            target_node_id: target,
        }
    }
}

/// Network adapter for global consensus
pub struct GlobalRaftNetworkAdapter<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Network manager for sending messages
    network_manager: Arc<NetworkManager<T, G>>,
    /// Network statistics
    stats: Arc<RwLock<NetworkStats>>,
    /// Target node ID
    target_node_id: NodeId,
}

impl<T, G> GlobalRaftNetworkAdapter<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new global Raft network adapter
    pub fn new(
        network_manager: Arc<NetworkManager<T, G>>,
        stats: Arc<RwLock<NetworkStats>>,
    ) -> Self {
        Self {
            network_manager,
            stats,
            target_node_id: NodeId::from_bytes(&[0u8; 32]).expect("valid bytes"), // This will be set per-connection
        }
    }
}

impl<T, G> RaftNetwork<GlobalTypeConfig> for GlobalRaftNetworkAdapter<T, G>
where
    T: Transport,
    G: Governance,
{
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<GlobalTypeConfig>,
        option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<GlobalTypeConfig>,
        RPCError<GlobalTypeConfig, RaftError<GlobalTypeConfig>>,
    > {
        let timeout = option.hard_ttl();

        // Create the request message
        let message = GlobalAppendEntriesRequest(rpc);

        // Send request and wait for response
        let response: GlobalAppendEntriesResponse = self
            .network_manager
            .request_namespaced(
                super::messages::GLOBAL_CONSENSUS_NAMESPACE,
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

        // Extract the inner response
        Ok(response.0)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<GlobalTypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<GlobalTypeConfig>,
        RPCError<GlobalTypeConfig, RaftError<GlobalTypeConfig, InstallSnapshotError>>,
    > {
        let timeout = option.hard_ttl();

        // Create the request message
        let message = GlobalInstallSnapshotRequest(rpc);

        // Send request and wait for response
        let response: GlobalInstallSnapshotResponse = self
            .network_manager
            .request_namespaced(
                super::messages::GLOBAL_CONSENSUS_NAMESPACE,
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

        // Extract the inner response
        Ok(response.0)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<GlobalTypeConfig>,
        option: RPCOption,
    ) -> Result<
        VoteResponse<GlobalTypeConfig>,
        RPCError<GlobalTypeConfig, RaftError<GlobalTypeConfig>>,
    > {
        let timeout = option.hard_ttl();

        // Create the request message
        let message = GlobalVoteRequest(rpc);

        // Send request and wait for response
        let response: GlobalVoteResponse = self
            .network_manager
            .request_namespaced(
                super::messages::GLOBAL_CONSENSUS_NAMESPACE,
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

        // Extract the inner response
        Ok(response.0)
    }
}
