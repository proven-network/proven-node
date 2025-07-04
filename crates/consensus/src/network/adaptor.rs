//! Raft network adapter
//!
//! This module provides the RaftAdapter and NetworkFactory that bridge
//! OpenRaft with our transport layer.

use crate::types::{NodeId, TypeConfig};

use std::collections::HashMap;
use std::sync::Arc;

use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use proven_governance::GovernanceNode;
use tokio::sync::{RwLock, mpsc, oneshot};
use uuid::Uuid;

// Type aliases for complex channel types
type RaftRequestSender = mpsc::UnboundedSender<(NodeId, Uuid, Box<RaftAdapterRequest>)>;
type RaftResponseReceiver = mpsc::UnboundedReceiver<(NodeId, Uuid, Box<RaftAdapterResponse>)>;
type ResponseSender = oneshot::Sender<Box<RaftAdapterResponse>>;
type ResponseReceiver = oneshot::Receiver<Box<RaftAdapterResponse>>;

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

/// RaftCorrelator manages correlation IDs and their associated response channels
/// This eliminates contention by providing direct mapping from correlation ID to response sender
#[derive(Clone)]
pub struct RaftCorrelator {
    /// Map of correlation IDs to response senders
    pending_responses: Arc<RwLock<HashMap<Uuid, ResponseSender>>>,
}

impl RaftCorrelator {
    /// Create a new RaftCorrelator
    pub fn new() -> Self {
        Self {
            pending_responses: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a correlation ID and return a oneshot receiver for the response
    pub(crate) async fn register(&self, correlation_id: Uuid) -> ResponseReceiver {
        let (tx, rx) = oneshot::channel();
        let mut pending = self.pending_responses.write().await;
        pending.insert(correlation_id, tx);
        rx
    }

    /// Send a response for a given correlation ID
    pub(crate) async fn send_response(
        &self,
        correlation_id: Uuid,
        response: Box<RaftAdapterResponse>,
    ) {
        let mut pending = self.pending_responses.write().await;
        if let Some(sender) = pending.remove(&correlation_id) {
            let _ = sender.send(response); // Ignore if receiver is dropped
        }
    }

    /// Clean up expired correlation IDs (for cases where responses never arrive)
    pub async fn cleanup_expired(&self, expired_ids: Vec<Uuid>) {
        let mut pending = self.pending_responses.write().await;
        for id in expired_ids {
            pending.remove(&id);
        }
    }

    /// Get count of pending responses (for debugging/monitoring)
    pub async fn pending_count(&self) -> usize {
        let pending = self.pending_responses.read().await;
        pending.len()
    }
}

impl Default for RaftCorrelator {
    fn default() -> Self {
        Self::new()
    }
}

/// Factory for creating Raft network instances
pub struct NetworkFactory {
    message_tx: RaftRequestSender,
    /// Correlator for managing response correlation
    correlator: RaftCorrelator,
}

impl NetworkFactory {
    /// Create a new Raft network factory
    pub(crate) fn new(message_tx: RaftRequestSender, message_rx: RaftResponseReceiver) -> Self {
        let correlator = RaftCorrelator::new();

        // Spawn the central message processing loop
        let correlator_clone = correlator.clone();
        tokio::spawn(async move {
            Self::message_processing_loop(message_rx, correlator_clone).await;
        });

        Self {
            message_tx,
            correlator,
        }
    }

    /// Central message processing loop that distributes responses via correlator
    async fn message_processing_loop(
        mut message_rx: RaftResponseReceiver,
        correlator: RaftCorrelator,
    ) {
        while let Some((_node_id, correlation_id, response)) = message_rx.recv().await {
            // Send response directly to the waiting correlation ID
            correlator.send_response(correlation_id, response).await;
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = RaftAdapter;

    async fn new_client(&mut self, target: NodeId, _node: &GovernanceNode) -> Self::Network {
        RaftAdapter {
            correlator: self.correlator.clone(),
            message_tx: self.message_tx.clone(),
            target_node_id: target,
        }
    }
}

/// Network interface for OpenRaft
/// Adapter that handles Raft protocol messages for a specific target node
/// This implements RaftNetwork and knows how to communicate with remote Raft instances
pub struct RaftAdapter {
    /// Correlator for handling response correlation
    correlator: RaftCorrelator,

    /// Channel for sending messages through transport
    message_tx: RaftRequestSender,

    /// Target node ID
    target_node_id: NodeId,
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

        // Register correlation ID and get oneshot receiver
        let response_rx = self.correlator.register(correlation_id).await;

        // Wait for response with timeout
        match tokio::time::timeout(timeout, response_rx).await {
            Ok(Ok(response)) => match *response {
                RaftAdapterResponse::AppendEntries(resp) => return Ok(resp),
                _ => {
                    return Err(RPCError::Network(openraft::error::NetworkError::new(
                        &std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Unexpected response type",
                        ),
                    )));
                }
            },
            Ok(Err(_)) => {
                return Err(RPCError::Network(openraft::error::NetworkError::new(
                    &std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        "Response sender dropped",
                    ),
                )));
            }
            Err(_) => {
                // Timeout - clean up the correlation ID
                self.correlator.cleanup_expired(vec![correlation_id]).await;
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

        // Register correlation ID and get oneshot receiver
        let response_rx = self.correlator.register(correlation_id).await;

        // Wait for response with timeout
        match tokio::time::timeout(timeout, response_rx).await {
            Ok(Ok(response)) => match *response {
                RaftAdapterResponse::InstallSnapshot(resp) => return Ok(resp),
                _ => {
                    return Err(RPCError::Network(openraft::error::NetworkError::new(
                        &std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Unexpected response type",
                        ),
                    )));
                }
            },
            Ok(Err(_)) => {
                return Err(RPCError::Network(openraft::error::NetworkError::new(
                    &std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        "Response sender dropped",
                    ),
                )));
            }
            Err(_) => {
                // Timeout - clean up the correlation ID
                self.correlator.cleanup_expired(vec![correlation_id]).await;
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

        // Register correlation ID and get oneshot receiver
        let response_rx = self.correlator.register(correlation_id).await;

        // Wait for response with timeout
        match tokio::time::timeout(timeout, response_rx).await {
            Ok(Ok(response)) => match *response {
                RaftAdapterResponse::Vote(resp) => return Ok(*resp),
                _ => {
                    return Err(RPCError::Network(openraft::error::NetworkError::new(
                        &std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Unexpected response type",
                        ),
                    )));
                }
            },
            Ok(Err(_)) => {
                return Err(RPCError::Network(openraft::error::NetworkError::new(
                    &std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        "Response sender dropped",
                    ),
                )));
            }
            Err(_) => {
                // Timeout - clean up the correlation ID
                self.correlator.cleanup_expired(vec![correlation_id]).await;
            }
        }

        Err(RPCError::Network(openraft::error::NetworkError::new(
            &std::io::Error::new(std::io::ErrorKind::TimedOut, "Request timeout"),
        )))
    }
}
