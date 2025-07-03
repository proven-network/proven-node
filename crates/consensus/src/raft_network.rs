//! OpenRaft network implementation for consensus messaging.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::error::ConsensusError;
use crate::transport::{ConsensusMessage, ConsensusTransport};
use crate::types::TypeConfig;

/// Default timeout for RPC requests
const RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Request/response tracking for async RPC calls
#[derive(Debug)]
struct PendingRequest {
    /// Response sender
    response_tx: oneshot::Sender<ConsensusMessage>,
    /// Request timestamp for timeout tracking
    timestamp: std::time::Instant,
}

/// Global response router for handling incoming raft responses
static RESPONSE_ROUTER: std::sync::LazyLock<Arc<RwLock<HashMap<Uuid, PendingRequest>>>> =
    std::sync::LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));

/// Type alias for the complex network cache type
type NetworkCache<T> = HashMap<String, Arc<ConsensusRaftNetwork<T>>>;

/// OpenRaft-specific message wrapper with request tracking
#[derive(Debug, Serialize, Deserialize)]
pub struct RaftMessageEnvelope {
    /// Unique request ID for response correlation
    pub request_id: Uuid,
    /// Whether this is a response to a previous request
    pub is_response: bool,
    /// The actual raft message payload
    pub payload: Vec<u8>,
    /// Message type for routing
    pub message_type: String,
}

/// Network factory for creating `OpenRaft` network instances
pub struct ConsensusRaftNetworkFactory<T>
where
    T: ConsensusTransport,
{
    /// Reference to our existing transport infrastructure
    consensus_transport: Arc<T>,
    /// Cache of network instances for different target nodes
    networks: Arc<RwLock<NetworkCache<T>>>,
    /// Sender for outbound network messages
    network_tx: mpsc::UnboundedSender<(String, ConsensusMessage)>,
}

impl<T> ConsensusRaftNetworkFactory<T>
where
    T: ConsensusTransport,
{
    /// Create a new network factory
    #[must_use]
    pub fn new(
        consensus_transport: Arc<T>,
        network_tx: mpsc::UnboundedSender<(String, ConsensusMessage)>,
    ) -> Self {
        Self {
            consensus_transport,
            networks: Arc::new(RwLock::new(HashMap::new())),
            network_tx,
        }
    }
}

impl<T> RaftNetworkFactory<TypeConfig> for ConsensusRaftNetworkFactory<T>
where
    T: ConsensusTransport,
{
    type Network = ConsensusRaftNetwork<T>;

    /// Create or get a network instance for communicating with a specific target node
    async fn new_client(&mut self, target: String, _node: &BasicNode) -> Self::Network {
        // Check if we already have a network instance for this target
        {
            let networks = self.networks.read().await;
            if let Some(network) = networks.get(&target) {
                return (**network).clone();
            }
        }

        // Create a new network instance for this target
        let network = ConsensusRaftNetwork::new(
            self.consensus_transport.clone(),
            target.clone(),
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
pub struct ConsensusRaftNetwork<T>
where
    T: ConsensusTransport,
{
    /// Reference to our consensus transport
    consensus_transport: Arc<T>,
    /// Target node ID for this network instance
    target_node_id: String,
    /// Sender for outbound network messages
    network_tx: mpsc::UnboundedSender<(String, ConsensusMessage)>,
}

impl<T> ConsensusRaftNetwork<T>
where
    T: ConsensusTransport,
{
    /// Create a new network instance for a specific target
    #[must_use]
    pub const fn new(
        consensus_transport: Arc<T>,
        target_node_id: String,
        network_tx: mpsc::UnboundedSender<(String, ConsensusMessage)>,
    ) -> Self {
        Self {
            consensus_transport,
            target_node_id,
            network_tx,
        }
    }

    /// Send a request and wait for response with timeout
    #[allow(clippy::result_large_err)]
    #[allow(clippy::too_many_lines)]
    async fn send_request_with_response<Req, Resp>(
        &self,
        request: &Req,
        message_type: &str,
        response_type: &str,
    ) -> Result<Resp, RPCError<TypeConfig, RaftError<TypeConfig>>>
    where
        Req: Serialize + Send + Sync,
        Resp: for<'de> Deserialize<'de> + Send + Sync,
    {
        let request_id = Uuid::new_v4();

        // Serialize the request
        let payload = serde_json::to_vec(request).map_err(|e| {
            RPCError::Network(openraft::error::NetworkError::new(
                &ConsensusError::InvalidMessage(format!("Failed to serialize request: {e}")),
            ))
        })?;

        // Create request envelope
        let envelope = RaftMessageEnvelope {
            request_id,
            is_response: false,
            payload,
            message_type: message_type.to_string(),
        };

        // Serialize envelope
        let envelope_data = serde_json::to_vec(&envelope).map_err(|e| {
            RPCError::Network(openraft::error::NetworkError::new(
                &ConsensusError::InvalidMessage(format!("Failed to serialize envelope: {e}")),
            ))
        })?;

        // Create response channel
        let (response_tx, response_rx) = oneshot::channel();

        // Register pending request
        {
            let mut router = RESPONSE_ROUTER.write().await;
            router.insert(
                request_id,
                PendingRequest {
                    response_tx,
                    timestamp: std::time::Instant::now(),
                },
            );
        }

        // Determine the correct ConsensusMessage type
        let consensus_msg = match message_type {
            "vote" => ConsensusMessage::RaftVote(envelope_data),
            "append_entries" => ConsensusMessage::RaftAppendEntries(envelope_data),
            "install_snapshot" => ConsensusMessage::RaftInstallSnapshot(envelope_data),
            _ => ConsensusMessage::Data(envelope_data.into()),
        };

        // Send the message through transport
        if let Err(e) = self
            .consensus_transport
            .send_message(&self.target_node_id, consensus_msg)
            .await
        {
            // Cleanup pending request
            RESPONSE_ROUTER.write().await.remove(&request_id);

            return Err(RPCError::Network(openraft::error::NetworkError::new(&e)));
        }

        debug!(
            "Sent {} request to node {} with ID {}",
            message_type, self.target_node_id, request_id
        );

        // Wait for response with timeout
        let response = match tokio::time::timeout(RPC_TIMEOUT, response_rx).await {
            Ok(Ok(response_msg)) => response_msg,
            Ok(Err(_)) => {
                // Channel was closed
                return Err(RPCError::Network(openraft::error::NetworkError::new(
                    &ConsensusError::Network("Response channel closed".into()),
                )));
            }
            Err(_) => {
                // Timeout
                RESPONSE_ROUTER.write().await.remove(&request_id);

                return Err(RPCError::Network(openraft::error::NetworkError::new(
                    &ConsensusError::Network(
                        format!(
                            "Request timeout after {:?} for {} to node {}",
                            RPC_TIMEOUT, message_type, self.target_node_id
                        )
                        .into(),
                    ),
                )));
            }
        };

        // Extract response data based on message type
        let response_data = match (&response, response_type) {
            (ConsensusMessage::RaftVoteResponse(data), "vote_response")
            | (ConsensusMessage::RaftAppendEntriesResponse(data), "append_entries_response")
            | (ConsensusMessage::RaftInstallSnapshotResponse(data), "install_snapshot_response") => {
                data
            }
            (ConsensusMessage::Data(data), _) => data.as_ref(),
            _ => {
                return Err(RPCError::Network(openraft::error::NetworkError::new(
                    &ConsensusError::InvalidMessage(format!(
                        "Unexpected response type for {response_type}: {response:?}"
                    )),
                )));
            }
        };

        // Deserialize envelope
        let response_envelope: RaftMessageEnvelope = serde_json::from_slice(response_data)
            .map_err(|e| {
                RPCError::Network(openraft::error::NetworkError::new(
                    &ConsensusError::InvalidMessage(format!(
                        "Failed to deserialize response envelope: {e}"
                    )),
                ))
            })?;

        // Deserialize actual response
        let response: Resp = serde_json::from_slice(&response_envelope.payload).map_err(|e| {
            RPCError::Network(openraft::error::NetworkError::new(
                &ConsensusError::InvalidMessage(format!("Failed to deserialize response: {e}")),
            ))
        })?;

        debug!(
            "Received {} response from node {} for request {}",
            response_type, self.target_node_id, request_id
        );

        Ok(response)
    }
}

impl<T> RaftNetwork<TypeConfig> for ConsensusRaftNetwork<T>
where
    T: ConsensusTransport,
{
    /// Send a vote request to the target node
    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>> {
        debug!("Sending vote request to node {}", self.target_node_id);

        self.send_request_with_response(&rpc, "vote", "vote_response")
            .await
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

        self.send_request_with_response(&rpc, "append_entries", "append_entries_response")
            .await
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

        match self
            .send_request_with_response(&rpc, "install_snapshot", "install_snapshot_response")
            .await
        {
            Ok(response) => Ok(response),
            Err(err) => {
                // Convert any error to a network error for install_snapshot
                let network_error = match err {
                    RPCError::Network(net_err) => net_err,
                    _ => openraft::error::NetworkError::new(&ConsensusError::Network(
                        format!("Install snapshot failed: {err:?}").into(),
                    )),
                };
                Err(RPCError::Network(network_error))
            }
        }
    }
}

impl<T> Clone for ConsensusRaftNetwork<T>
where
    T: ConsensusTransport,
{
    fn clone(&self) -> Self {
        Self {
            consensus_transport: self.consensus_transport.clone(),
            target_node_id: self.target_node_id.clone(),
            network_tx: self.network_tx.clone(),
        }
    }
}

impl<T> std::fmt::Debug for ConsensusRaftNetwork<T>
where
    T: ConsensusTransport,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsensusRaftNetwork")
            .field("target_node_id", &self.target_node_id)
            .finish_non_exhaustive()
    }
}

/// Handle incoming raft response and route to appropriate pending request
pub async fn handle_raft_response(response_message: ConsensusMessage) {
    let response_data = match &response_message {
        ConsensusMessage::RaftVoteResponse(data)
        | ConsensusMessage::RaftAppendEntriesResponse(data)
        | ConsensusMessage::RaftInstallSnapshotResponse(data) => data.as_ref(),
        ConsensusMessage::Data(data) => data.as_ref(),
        _ => {
            warn!("Received non-response raft message: {:?}", response_message);
            return;
        }
    };

    // Try to deserialize envelope
    let envelope: RaftMessageEnvelope = match serde_json::from_slice(response_data) {
        Ok(env) => env,
        Err(e) => {
            warn!("Failed to deserialize raft response envelope: {}", e);
            return;
        }
    };

    if !envelope.is_response {
        debug!("Received raft request (not response), ignoring in response handler");
        return;
    }

    // Find and remove pending request
    let pending_request = {
        let mut router = RESPONSE_ROUTER.write().await;
        router.remove(&envelope.request_id)
    };

    if let Some(pending) = pending_request {
        debug!("Routing response for request ID: {}", envelope.request_id);

        // Send response to waiting request
        if pending.response_tx.send(response_message).is_err() {
            warn!(
                "Failed to send response - receiver was dropped for request ID: {}",
                envelope.request_id
            );
        }
    } else {
        warn!(
            "Received response for unknown request ID: {}",
            envelope.request_id
        );
    }
}

/// Clean up expired pending requests
pub async fn cleanup_expired_requests() {
    let mut router = RESPONSE_ROUTER.write().await;
    let now = std::time::Instant::now();

    router.retain(|request_id, pending| {
        let expired = now.duration_since(pending.timestamp) > RPC_TIMEOUT * 2;
        if expired {
            debug!("Cleaning up expired request: {}", request_id);
        }
        !expired
    });
}
