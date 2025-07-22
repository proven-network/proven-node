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

use proven_logger::debug;
use proven_network::NetworkManager;
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use super::messages::{GlobalConsensusMessage, GlobalConsensusResponse};
use crate::consensus::global::GlobalTypeConfig;

/// Factory for creating global Raft network instances
pub struct GlobalNetworkFactory<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    network_manager: Arc<NetworkManager<T, G>>,
}

impl<T, G> GlobalNetworkFactory<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Create a new global network factory
    pub fn new(network_manager: Arc<NetworkManager<T, G>>) -> Self {
        Self { network_manager }
    }
}

impl<T, G> RaftNetworkFactory<GlobalTypeConfig> for GlobalNetworkFactory<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    type Network = GlobalRaftNetworkAdapter<T, G>;

    async fn new_client(&mut self, target: NodeId, _node: &proven_topology::Node) -> Self::Network {
        GlobalRaftNetworkAdapter {
            network_manager: self.network_manager.clone(),
            target_node_id: target,
        }
    }
}

/// Network adapter for global consensus
pub struct GlobalRaftNetworkAdapter<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Network manager for sending messages
    network_manager: Arc<NetworkManager<T, G>>,
    /// Target node ID
    target_node_id: NodeId,
}

impl<T, G> GlobalRaftNetworkAdapter<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Create a new global Raft network adapter
    pub fn new(network_manager: Arc<NetworkManager<T, G>>) -> Self {
        Self {
            network_manager,
            target_node_id: NodeId::from_bytes(&[0u8; 32]).expect("valid bytes"), // This will be set per-connection
        }
    }
}

impl<T, G> RaftNetwork<GlobalTypeConfig> for GlobalRaftNetworkAdapter<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
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
        let message = GlobalConsensusMessage::AppendEntries(rpc);

        // Send request and wait for response
        let response = self
            .network_manager
            .service_request(self.target_node_id.clone(), message, timeout)
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        // Extract the response
        match response {
            GlobalConsensusResponse::AppendEntries(resp) => Ok(resp),
            _ => Err(RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::InvalidData, "Unexpected response type"),
            ))),
        }
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
        let message = GlobalConsensusMessage::InstallSnapshot(rpc);

        // Send request and wait for response
        let response = self
            .network_manager
            .service_request(self.target_node_id.clone(), message, timeout)
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        // Extract the response
        match response {
            GlobalConsensusResponse::InstallSnapshot(resp) => Ok(resp),
            _ => Err(RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::InvalidData, "Unexpected response type"),
            ))),
        }
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
        let message = GlobalConsensusMessage::Vote(rpc);

        // Implement exponential backoff for transient network failures
        let mut attempt = 0;
        let max_attempts = 3;
        let mut backoff = tokio::time::Duration::from_millis(100);

        loop {
            match self
                .network_manager
                .service_request(self.target_node_id.clone(), message.clone(), timeout)
                .await
            {
                Ok(response) => {
                    // Extract the response
                    match response {
                        GlobalConsensusResponse::Vote(resp) => return Ok(resp),
                        _ => {
                            return Err(RPCError::Network(openraft::error::NetworkError::new(
                                &std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "Unexpected response type",
                                ),
                            )));
                        }
                    }
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= max_attempts {
                        return Err(RPCError::Network(openraft::error::NetworkError::new(&e)));
                    }

                    // Check if this is a "no handler" error that might be transient
                    let error_str = e.to_string();
                    if error_str.contains("No pending request found")
                        || error_str.contains("Handler not found")
                        || error_str.contains("not initialized")
                    {
                        debug!(
                            "Vote request to {} failed (attempt {}/{}), retrying with backoff: {}",
                            self.target_node_id, attempt, max_attempts, error_str
                        );
                        tokio::time::sleep(backoff).await;
                        backoff *= 2; // Exponential backoff
                    } else {
                        // Non-transient error, fail immediately
                        return Err(RPCError::Network(openraft::error::NetworkError::new(&e)));
                    }
                }
            }
        }
    }
}
