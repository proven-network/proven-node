//! Raft network adapter
//!
//! This module provides the RaftAdapter and NetworkFactory that bridge
//! OpenRaft with our transport layer.

use crate::types::{NodeId, TypeConfig};

use std::sync::Arc;
use std::time::Duration;

use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use proven_governance::GovernanceNode;
use tokio::sync::{RwLock, mpsc};
use uuid::Uuid;

// Type aliases for complex channel types
type RaftRequestSender = mpsc::UnboundedSender<(NodeId, Uuid, Box<RaftAdapterRequest>)>;
type RaftResponseReceiver = mpsc::UnboundedReceiver<(NodeId, Uuid, Box<RaftAdapterResponse>)>;

pub(crate) enum RaftAdapterRequest {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
    Vote(VoteRequest<TypeConfig>),
}

pub(crate) enum RaftAdapterResponse {
    AppendEntries(AppendEntriesResponse<TypeConfig>),
    InstallSnapshot(InstallSnapshotResponse<TypeConfig>),
    Vote(Box<VoteResponse<TypeConfig>>),
}

/// Factory for creating Raft network instances
pub struct NetworkFactory {
    message_tx: RaftRequestSender,
    /// Channel for receiving response messages
    message_rx: Arc<RwLock<RaftResponseReceiver>>,
}

impl NetworkFactory {
    /// Create a new Raft network factory
    pub(crate) fn new(message_tx: RaftRequestSender, message_rx: RaftResponseReceiver) -> Self {
        Self {
            message_tx,
            message_rx: Arc::new(RwLock::new(message_rx)),
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = RaftAdapter;

    async fn new_client(&mut self, target: NodeId, _node: &GovernanceNode) -> Self::Network {
        RaftAdapter {
            target_node_id: target,
            message_tx: self.message_tx.clone(),
            message_rx: self.message_rx.clone(),
        }
    }
}

/// Network interface for OpenRaft
/// Adapter that handles Raft protocol messages for a specific target node
/// This implements RaftNetwork and knows how to communicate with remote Raft instances
pub struct RaftAdapter {
    /// Target node ID
    target_node_id: NodeId,

    /// Channel for sending messages through transport
    message_tx: RaftRequestSender,

    /// Channel for receiving response messages
    message_rx: Arc<RwLock<RaftResponseReceiver>>,
}

impl RaftNetwork<TypeConfig> for RaftAdapter {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>>
    {
        let correlation_id = Uuid::new_v4();
        let timeout = option.hard_ttl();

        // Send request through channel
        if let Err(e) = self.message_tx.send((
            self.target_node_id.clone(),
            correlation_id,
            Box::new(RaftAdapterRequest::AppendEntries(rpc.clone())),
        )) {
            return Err(RPCError::Network(openraft::error::NetworkError::new(&e)));
        }

        // Wait for response with timeout
        let start_time = tokio::time::Instant::now();
        let mut rx = self.message_rx.write().await;

        while start_time.elapsed() < timeout {
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some((node_id, resp_id, response))) => {
                    if node_id == self.target_node_id && resp_id == correlation_id {
                        match *response {
                            RaftAdapterResponse::AppendEntries(resp) => return Ok(resp),
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
                    // Not our response, continue waiting
                }
                Ok(None) => {
                    return Err(RPCError::Network(openraft::error::NetworkError::new(
                        &std::io::Error::new(
                            std::io::ErrorKind::ConnectionAborted,
                            "Channel closed",
                        ),
                    )));
                }
                Err(_) => {
                    // Timeout on recv, check if total timeout exceeded
                    continue;
                }
            }
        }

        Err(RPCError::Network(openraft::error::NetworkError::new(
            &std::io::Error::new(std::io::ErrorKind::TimedOut, "Request timeout"),
        )))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<TypeConfig>,
        RPCError<TypeConfig, RaftError<TypeConfig, InstallSnapshotError>>,
    > {
        let correlation_id = Uuid::new_v4();
        let timeout = option.hard_ttl();

        // Send request through channel
        if let Err(e) = self.message_tx.send((
            self.target_node_id.clone(),
            correlation_id,
            Box::new(RaftAdapterRequest::InstallSnapshot(rpc.clone())),
        )) {
            return Err(RPCError::Network(openraft::error::NetworkError::new(&e)));
        }

        // Wait for response with timeout
        let start_time = tokio::time::Instant::now();
        let mut rx = self.message_rx.write().await;

        while start_time.elapsed() < timeout {
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some((node_id, resp_id, response))) => {
                    if node_id == self.target_node_id && resp_id == correlation_id {
                        match *response {
                            RaftAdapterResponse::InstallSnapshot(resp) => return Ok(resp),
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
                    // Not our response, continue waiting
                }
                Ok(None) => {
                    return Err(RPCError::Network(openraft::error::NetworkError::new(
                        &std::io::Error::new(
                            std::io::ErrorKind::ConnectionAborted,
                            "Channel closed",
                        ),
                    )));
                }
                Err(_) => {
                    // Timeout on recv, check if total timeout exceeded
                    continue;
                }
            }
        }

        Err(RPCError::Network(openraft::error::NetworkError::new(
            &std::io::Error::new(std::io::ErrorKind::TimedOut, "Request timeout"),
        )))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>> {
        let correlation_id = Uuid::new_v4();
        let timeout = option.hard_ttl();

        // Send request through channel
        if let Err(e) = self.message_tx.send((
            self.target_node_id.clone(),
            correlation_id,
            Box::new(RaftAdapterRequest::Vote(rpc.clone())),
        )) {
            return Err(RPCError::Network(openraft::error::NetworkError::new(&e)));
        }

        // Wait for response with timeout
        let start_time = tokio::time::Instant::now();
        let mut rx = self.message_rx.write().await;

        while start_time.elapsed() < timeout {
            match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                Ok(Some((node_id, resp_id, response))) => {
                    if node_id == self.target_node_id && resp_id == correlation_id {
                        match *response {
                            RaftAdapterResponse::Vote(resp) => return Ok(*resp),
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
                    // Not our response, continue waiting
                }
                Ok(None) => {
                    return Err(RPCError::Network(openraft::error::NetworkError::new(
                        &std::io::Error::new(
                            std::io::ErrorKind::ConnectionAborted,
                            "Channel closed",
                        ),
                    )));
                }
                Err(_) => {
                    // Timeout on recv, check if total timeout exceeded
                    continue;
                }
            }
        }

        Err(RPCError::Network(openraft::error::NetworkError::new(
            &std::io::Error::new(std::io::ErrorKind::TimedOut, "Request timeout"),
        )))
    }
}
