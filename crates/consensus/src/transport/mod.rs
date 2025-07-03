//! Transport abstraction layer for consensus networking.
//!
//! This module provides a pluggable interface for different networking implementations,
//! allowing consensus to work with TCP (for tests) or WebSocket (for production).

use std::fmt::Debug;
use std::net::SocketAddr;
use std::time::SystemTime;

use async_trait::async_trait;
use axum::Router;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::error::ConsensusError;

pub mod tcp;
pub mod websocket;

/// Transport abstraction for consensus networking.
///
/// This trait allows different networking implementations (TCP, WebSocket, etc.)
/// to be used with the consensus system while providing a uniform interface.
#[async_trait]
pub trait ConsensusTransport: Debug + Send + Sync + 'static {
    /// Send a message to a specific peer node.
    ///
    /// # Arguments
    ///
    /// * `target_node_id` - The ID of the target peer node
    /// * `message` - The consensus message to send
    ///
    /// # Errors
    ///
    /// Returns `ConsensusError` if the message cannot be sent.
    async fn send_message(
        &self,
        target_node_id: &str,
        message: ConsensusMessage,
    ) -> Result<(), ConsensusError>;

    /// Get a sender that can be used to receive incoming messages from peers.
    ///
    /// The transport will send incoming messages to this sender.
    fn get_message_sender(&self) -> mpsc::UnboundedSender<(String, ConsensusMessage)>;

    /// Get information about currently connected peers.
    ///
    /// # Errors
    ///
    /// Returns `ConsensusError` if peer information cannot be retrieved.
    async fn get_connected_peers(&self) -> Result<Vec<PeerConnection>, ConsensusError>;

    /// Start the transport layer.
    ///
    /// This should begin listening for connections and be ready to send/receive messages.
    ///
    /// # Errors
    ///
    /// Returns `ConsensusError` if the transport cannot be started.
    async fn start(&self) -> Result<(), ConsensusError>;

    /// Shutdown the transport layer.
    ///
    /// This should close all connections and stop the transport.
    ///
    /// # Errors
    ///
    /// Returns `ConsensusError` if the transport cannot be shutdown gracefully.
    async fn shutdown(&self) -> Result<(), ConsensusError>;

    /// Discover existing clusters by querying peers.
    ///
    /// # Errors
    ///
    /// Returns `ConsensusError` if cluster discovery fails.
    async fn discover_existing_clusters(
        &self,
    ) -> Result<Vec<ClusterDiscoveryResponse>, ConsensusError>;

    /// Get the local address this transport is bound to.
    fn local_address(&self) -> Option<SocketAddr>;

    /// Create an axum Router for HTTP-based transports.
    ///
    /// This method allows transports that use HTTP (like WebSocket) to provide
    /// their routes to be mounted into the main HTTP server.
    ///
    /// # Panics
    ///
    /// For transports that don't use HTTP (like TCP), this method should panic
    /// since it should never be called.
    ///
    /// # Returns
    ///
    /// Returns an axum Router that can be mounted into an HTTP server.
    fn create_router(&self) -> Router;
}

/// Consensus message types for inter-node communication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusMessage {
    /// Cluster discovery request.
    ClusterDiscovery(ClusterDiscoveryRequest),
    /// Cluster discovery response.
    ClusterDiscoveryResponse(ClusterDiscoveryResponse),
    /// Raw data message.
    Data(Bytes),
    /// `OpenRaft` vote request.
    RaftVote(Vec<u8>), // Serialized VoteRequest
    /// `OpenRaft` vote response.
    RaftVoteResponse(Vec<u8>), // Serialized VoteResponse
    /// `OpenRaft` append entries request.
    RaftAppendEntries(Vec<u8>), // Serialized AppendEntriesRequest
    /// `OpenRaft` append entries response.
    RaftAppendEntriesResponse(Vec<u8>), // Serialized AppendEntriesResponse
    /// `OpenRaft` install snapshot request.
    RaftInstallSnapshot(Vec<u8>), // Serialized InstallSnapshotRequest
    /// `OpenRaft` install snapshot response.
    RaftInstallSnapshotResponse(Vec<u8>), // Serialized InstallSnapshotResponse
    /// Basic placeholder messages for initial transport implementation.
    Vote,
    /// Append entries message type.
    AppendEntries,
    /// Install snapshot message type.
    InstallSnapshot,
}

/// Cluster discovery request message
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterDiscoveryRequest {
    /// Requesting node's ID
    pub requester_id: String,
    /// Request timestamp
    pub timestamp: u64,
}

/// Cluster discovery response message
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterDiscoveryResponse {
    /// Current leader ID (if known)
    pub current_leader: Option<String>,
    /// Current cluster membership size
    pub cluster_size: Option<usize>,
    /// Current Raft term (if in cluster)
    pub current_term: Option<u64>,
    /// Whether this node is part of an active cluster
    pub has_active_cluster: bool,
    /// Responding node's ID
    pub responder_id: String,
    /// Response timestamp
    pub timestamp: u64,
}

/// Connection status for a peer.
#[derive(Clone, Debug)]
pub struct PeerConnection {
    /// Socket address.
    pub address: SocketAddr,
    /// Whether attestation has been verified.
    pub attestation_verified: bool,
    /// Whether the connection is active.
    pub connected: bool,
    /// Last activity timestamp.
    pub last_activity: SystemTime,
    /// Node ID in consensus protocol.
    pub node_id: String,
}
