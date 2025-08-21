//! Network adaptor for group consensus Raft integration

use std::sync::Arc;
use std::time::Duration;

use openraft::{
    error::{InstallSnapshotError, RPCError, RaftError},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use proven_attestation::Attestor;

use proven_network::NetworkManager;
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

use super::messages::{GroupConsensusMessage, GroupConsensusServiceResponse};
use crate::consensus::group::GroupTypeConfig;
use crate::foundation::types::ConsensusGroupId;

/// Factory for creating group Raft network instances
pub struct GroupNetworkFactory<T, G, A>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
{
    network_manager: Arc<NetworkManager<T, G, A>>,
    group_id: ConsensusGroupId,
}

impl<T, G, A> GroupNetworkFactory<T, G, A>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
{
    /// Create a new group network factory
    pub fn new(network_manager: Arc<NetworkManager<T, G, A>>, group_id: ConsensusGroupId) -> Self {
        Self {
            network_manager,
            group_id,
        }
    }
}

impl<T, G, A> RaftNetworkFactory<GroupTypeConfig> for GroupNetworkFactory<T, G, A>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
{
    type Network = GroupRaftNetworkAdapter<T, G, A>;

    async fn new_client(&mut self, target: NodeId, _node: &proven_topology::Node) -> Self::Network {
        GroupRaftNetworkAdapter {
            network_manager: self.network_manager.clone(),
            group_id: self.group_id,
            target_node_id: target,
        }
    }
}

/// Network adapter for group consensus
pub struct GroupRaftNetworkAdapter<T, G, A>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
{
    /// Network manager for sending messages
    network_manager: Arc<NetworkManager<T, G, A>>,
    /// Group ID
    group_id: ConsensusGroupId,
    /// Target node ID
    target_node_id: NodeId,
}

impl<T, G, A> RaftNetwork<GroupTypeConfig> for GroupRaftNetworkAdapter<T, G, A>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
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

        // Ensure minimum timeout of 1 second for network operations
        let timeout = timeout.max(Duration::from_secs(1));

        // Create the request message
        let message = GroupConsensusMessage::AppendEntries {
            group_id: self.group_id,
            request: rpc,
        };

        // Send request and wait for response
        let response = self
            .network_manager
            .request_with_timeout(self.target_node_id, message, timeout)
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        // Extract the response
        match response {
            GroupConsensusServiceResponse::AppendEntries { group_id, response } => {
                if group_id == self.group_id {
                    Ok(response)
                } else {
                    Err(RPCError::Network(openraft::error::NetworkError::new(
                        &std::io::Error::new(std::io::ErrorKind::InvalidData, "Group ID mismatch"),
                    )))
                }
            }
            _ => Err(RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::InvalidData, "Unexpected response type"),
            ))),
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

        // Ensure minimum timeout of 5 seconds for snapshot operations
        let timeout = timeout.max(Duration::from_secs(5));

        // Create the request message
        let message = GroupConsensusMessage::InstallSnapshot {
            group_id: self.group_id,
            request: rpc,
        };

        // Send request and wait for response
        let response = self
            .network_manager
            .request_with_timeout(self.target_node_id, message, timeout)
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        // Extract the response
        match response {
            GroupConsensusServiceResponse::InstallSnapshot { group_id, response } => {
                if group_id == self.group_id {
                    Ok(response)
                } else {
                    Err(RPCError::Network(openraft::error::NetworkError::new(
                        &std::io::Error::new(std::io::ErrorKind::InvalidData, "Group ID mismatch"),
                    )))
                }
            }
            _ => Err(RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::InvalidData, "Unexpected response type"),
            ))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<GroupTypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<GroupTypeConfig>, RPCError<GroupTypeConfig, RaftError<GroupTypeConfig>>>
    {
        let timeout = option.hard_ttl();

        // Ensure minimum timeout of 1 second for vote operations
        let timeout = timeout.max(Duration::from_secs(1));

        // Create the request message
        let message = GroupConsensusMessage::Vote {
            group_id: self.group_id,
            request: rpc,
        };

        // Implement exponential backoff for transient network failures
        let mut attempt = 0;
        let max_attempts = 3;
        let mut backoff = tokio::time::Duration::from_millis(100);

        loop {
            match self
                .network_manager
                .request_with_timeout(self.target_node_id, message.clone(), timeout)
                .await
            {
                Ok(response) => {
                    // Extract the response
                    match response {
                        GroupConsensusServiceResponse::Vote { group_id, response } => {
                            if group_id == self.group_id {
                                return Ok(response);
                            } else {
                                return Err(RPCError::Network(openraft::error::NetworkError::new(
                                    &std::io::Error::new(
                                        std::io::ErrorKind::InvalidData,
                                        "Group ID mismatch",
                                    ),
                                )));
                            }
                        }
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
                        tracing::debug!(
                            "Group vote request to {} for group {:?} failed (attempt {}/{}), retrying with backoff: {}",
                            self.target_node_id,
                            self.group_id,
                            attempt,
                            max_attempts,
                            error_str
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
