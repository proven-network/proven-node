//! Pure networking manager
//!
//! This module handles pure networking concerns: transport management,
//! COSE signing/verification, message serialization/deserialization,
//! and message routing.

use super::cose::CoseMetadata;
use super::transport::{NetworkTransport, tcp::TcpTransport, websocket::WebSocketTransport};
use crate::NodeId;
use crate::error::{Error, NetworkError};
use crate::network::attestation::AttestationVerifier;
use crate::network::cose::CoseHandler;
use crate::network::messages::{ApplicationMessage, Message, RaftMessage};
use crate::network::verification::{ConnectionVerification, ConnectionVerifier};

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;
use proven_attestation::Attestor;
use proven_governance::Governance;
use tokio::sync::mpsc;
use tracing::{debug, error, info};
use uuid::Uuid;

/// Message handler callback for processing incoming messages
pub type MessageHandler = Arc<dyn Fn(NodeId, ApplicationMessage, Option<Uuid>) + Send + Sync>;

/// Raft message handler callback for processing incoming Raft messages
pub type RaftMessageHandler = Arc<dyn Fn(NodeId, RaftMessage, Option<Uuid>) + Send + Sync>;

// Type aliases for complex channel types
type IncomingMessageReceiver =
    Arc<tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<(NodeId, Bytes)>>>>;
type AppMessageReceiver = Arc<
    tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<(NodeId, ApplicationMessage, Option<Uuid>)>>>,
>;
type RaftMessageReceiver =
    Arc<tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<(NodeId, RaftMessage, Option<Uuid>)>>>>;
type OutgoingMessageReceiver =
    Arc<tokio::sync::Mutex<Option<mpsc::UnboundedReceiver<(NodeId, Message, Option<Uuid>)>>>>;
type ResponseReceiver = Arc<
    tokio::sync::Mutex<
        Option<mpsc::UnboundedReceiver<(Uuid, Box<dyn std::any::Any + Send + Sync>)>>,
    >,
>;

/// Channel structure for message routing in NetworkManager
#[derive(Debug)]
struct NetworkChannels {
    // Incoming message distribution
    incoming_message_tx: mpsc::UnboundedSender<(NodeId, Bytes)>,
    incoming_message_rx: IncomingMessageReceiver,

    // Application message processing
    app_message_tx: mpsc::UnboundedSender<(NodeId, ApplicationMessage, Option<Uuid>)>,
    app_message_rx: AppMessageReceiver,

    // Raft message processing
    raft_message_tx: mpsc::UnboundedSender<(NodeId, RaftMessage, Option<Uuid>)>,
    raft_message_rx: RaftMessageReceiver,

    // Outgoing message sending
    outgoing_message_tx: mpsc::UnboundedSender<(NodeId, Message, Option<Uuid>)>,
    outgoing_message_rx: OutgoingMessageReceiver,

    // Response correlation
    response_tx: mpsc::UnboundedSender<(Uuid, Box<dyn std::any::Any + Send + Sync>)>,
    response_rx: ResponseReceiver,
}

impl NetworkChannels {
    fn new() -> Self {
        let (incoming_message_tx, incoming_message_rx) = mpsc::unbounded_channel();
        let (app_message_tx, app_message_rx) = mpsc::unbounded_channel();
        let (raft_message_tx, raft_message_rx) = mpsc::unbounded_channel();
        let (outgoing_message_tx, outgoing_message_rx) = mpsc::unbounded_channel();
        let (response_tx, response_rx) = mpsc::unbounded_channel();

        Self {
            incoming_message_tx,
            incoming_message_rx: Arc::new(tokio::sync::Mutex::new(Some(incoming_message_rx))),
            app_message_tx,
            app_message_rx: Arc::new(tokio::sync::Mutex::new(Some(app_message_rx))),
            raft_message_tx,
            raft_message_rx: Arc::new(tokio::sync::Mutex::new(Some(raft_message_rx))),
            outgoing_message_tx,
            outgoing_message_rx: Arc::new(tokio::sync::Mutex::new(Some(outgoing_message_rx))),
            response_tx,
            response_rx: Arc::new(tokio::sync::Mutex::new(Some(response_rx))),
        }
    }
}

/// Pure networking manager that handles only transport and message routing
pub struct NetworkManager<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Our local node ID
    local_node_id: NodeId,

    /// Transport for networking
    transport: Arc<dyn NetworkTransport>,

    /// COSE handler for message signing/verification
    cose_handler: Arc<CoseHandler>,

    /// Network channels for message routing
    network_channels: NetworkChannels,

    /// Topology manager reference (for message routing and peer management)
    topology: Arc<crate::topology::TopologyManager<G>>,

    /// Phantom data to ensure that the attestor type is used
    _marker: PhantomData<A>,
}

impl<G, A> NetworkManager<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Create a new NetworkManager focused on pure networking
    pub async fn new(
        signing_key: ed25519_dalek::SigningKey,
        governance: Arc<G>,
        attestor: Arc<A>,
        local_node_id: NodeId,
        transport_config: crate::config::TransportConfig,
        topology: Arc<crate::topology::TopologyManager<G>>,
    ) -> Result<Self, NetworkError> {
        let local_public_key = local_node_id.clone();

        // Create COSE handler
        let cose_handler = Arc::new(CoseHandler::new(signing_key));

        // Create attestation verifier
        let attestation_verifier = Arc::new(AttestationVerifier::new(
            (*governance).clone(),
            (*attestor).clone(),
        ));

        // Create connection verifier
        let connection_verifier: Arc<dyn ConnectionVerification> = Arc::new(
            ConnectionVerifier::new(attestation_verifier, cose_handler.clone(), local_public_key),
        );

        // Create transport based on configuration
        let transport: Arc<dyn NetworkTransport> = match transport_config {
            crate::config::TransportConfig::Tcp { listen_addr } => Arc::new(TcpTransport::new(
                listen_addr,
                connection_verifier,
                topology.clone(),
            )),
            crate::config::TransportConfig::WebSocket => Arc::new(WebSocketTransport::new(
                connection_verifier,
                topology.clone(),
            )),
        };

        // Create network channels
        let network_channels = NetworkChannels::new();

        Ok(Self {
            local_node_id,
            transport,
            cose_handler,
            network_channels,
            topology,
            _marker: PhantomData,
        })
    }

    /// Start the network transport and message handling loops
    pub async fn start_network(
        &self,
        app_message_handler: Option<MessageHandler>,
        raft_message_handler: Option<RaftMessageHandler>,
    ) -> Result<(), NetworkError> {
        let (transport_tx, transport_rx) = mpsc::unbounded_channel::<(NodeId, Bytes)>();

        // Start background tasks
        let mut task_handles = Vec::new();

        // Task 1: Message Router Task
        let incoming_rx = self
            .network_channels
            .incoming_message_rx
            .lock()
            .await
            .take()
            .ok_or(NetworkError::ConnectionFailed {
                peer: self.local_node_id.clone(),
                reason: "Incoming message receiver already taken".to_string(),
            })?;
        let router_task = tokio::spawn(Self::message_router_loop(
            incoming_rx,
            self.network_channels.app_message_tx.clone(),
            self.network_channels.raft_message_tx.clone(),
            self.cose_handler.clone(),
        ));
        task_handles.push(router_task);

        // Task 2: Application Message Processor Task (if handler provided)
        if let Some(handler) = app_message_handler {
            let app_rx = self
                .network_channels
                .app_message_rx
                .lock()
                .await
                .take()
                .ok_or(NetworkError::ConnectionFailed {
                    peer: self.local_node_id.clone(),
                    reason: "App message receiver already taken".to_string(),
                })?;
            let app_processor_task =
                tokio::spawn(Self::application_message_processor_loop(app_rx, handler));
            task_handles.push(app_processor_task);
        }

        // Task 3: Raft Message Processor Task (if handler provided)
        if let Some(handler) = raft_message_handler {
            let raft_rx = self
                .network_channels
                .raft_message_rx
                .lock()
                .await
                .take()
                .ok_or(NetworkError::ConnectionFailed {
                    peer: self.local_node_id.clone(),
                    reason: "Raft message receiver already taken".to_string(),
                })?;
            let raft_processor_task =
                tokio::spawn(Self::raft_message_processor_loop(raft_rx, handler));
            task_handles.push(raft_processor_task);
        }

        // Task 4: Outgoing Message Sender Task
        let outgoing_rx = self
            .network_channels
            .outgoing_message_rx
            .lock()
            .await
            .take()
            .ok_or(NetworkError::ConnectionFailed {
                peer: self.local_node_id.clone(),
                reason: "Outgoing message receiver already taken".to_string(),
            })?;
        let sender_task = tokio::spawn(Self::outgoing_message_sender_loop(
            outgoing_rx,
            transport_tx.clone(),
            self.cose_handler.clone(),
            self.topology.clone(),
        ));
        task_handles.push(sender_task);

        // Task 5: Transport Message Sender Task
        let transport_clone = self.transport.clone();
        let transport_sender_task = tokio::spawn(async move {
            let mut transport_rx = transport_rx;
            debug!("Transport sender task started");
            while let Some((target_node_id, data)) = transport_rx.recv().await {
                info!(
                    "Transport sender: Sending {} bytes to {}",
                    data.len(),
                    target_node_id
                );
                if let Err(e) = transport_clone.send_bytes(&target_node_id, data).await {
                    error!("Failed to send message to {}: {}", target_node_id, e);
                } else {
                    info!("Transport sender: Successfully sent to {}", target_node_id);
                }
            }
            debug!("Transport sender task exited");
        });
        task_handles.push(transport_sender_task);

        // Create the simple transport handler that forwards to the router
        let message_handler = self.create_simple_transport_handler();

        // Start the transport listener
        self.transport.start_listener(message_handler).await?;

        info!(
            "NetworkManager started transport listener for node {}",
            self.local_node_id
        );
        Ok(())
    }

    /// Create a simple transport handler that forwards messages to the router
    pub fn create_simple_transport_handler(&self) -> super::transport::MessageHandler {
        let incoming_tx = self.network_channels.incoming_message_tx.clone();

        Arc::new(move |sender_id: NodeId, data: Bytes| {
            let incoming_tx = incoming_tx.clone();
            tokio::spawn(async move {
                if let Err(e) = incoming_tx.send((sender_id.clone(), data)) {
                    error!(
                        "Failed to forward incoming message from {} to router: {}",
                        sender_id, e
                    );
                }
            });
        })
    }

    /// Send a message to a target node
    pub async fn send_message(
        &self,
        target_node_id: NodeId,
        message: Message,
    ) -> Result<(), Error> {
        debug!(
            "NetworkManager sending message to {}: {:?}",
            target_node_id, message
        );
        self.network_channels
            .outgoing_message_tx
            .send((target_node_id, message, None))
            .map_err(|_| Error::InvalidMessage("Outgoing message channel closed".to_string()))
    }

    /// Send a message with correlation ID
    pub async fn send_message_with_correlation(
        &self,
        target_node_id: NodeId,
        message: Message,
        correlation_id: Uuid,
    ) -> Result<(), Error> {
        debug!(
            "NetworkManager sending message to {} with correlation {}: {:?}",
            target_node_id, correlation_id, message
        );
        self.network_channels
            .outgoing_message_tx
            .send((target_node_id, message, Some(correlation_id)))
            .map_err(|_| Error::InvalidMessage("Outgoing message channel closed".to_string()))
    }

    /// Get connected peers from transport
    pub async fn get_connected_peers(&self) -> Result<Vec<(NodeId, bool, SystemTime)>, Error> {
        self.transport
            .get_connected_peers()
            .await
            .map_err(Error::Network)
    }

    /// Shutdown the underlying transport
    pub async fn shutdown(&self) -> Result<(), NetworkError> {
        self.transport.shutdown().await
    }

    /// Get the response channel sender for correlation
    pub fn get_response_sender(
        &self,
    ) -> mpsc::UnboundedSender<(Uuid, Box<dyn std::any::Any + Send + Sync>)> {
        self.network_channels.response_tx.clone()
    }

    /// Take the response channel receiver for correlation processing
    pub async fn take_response_receiver(
        &self,
    ) -> Option<mpsc::UnboundedReceiver<(Uuid, Box<dyn std::any::Any + Send + Sync>)>> {
        self.network_channels.response_rx.lock().await.take()
    }

    /// Check if the underlying transport supports HTTP integration
    pub fn supports_http_integration(&self) -> bool {
        use super::transport::websocket::WebSocketTransport;
        self.transport
            .as_any()
            .downcast_ref::<WebSocketTransport<G>>()
            .is_some()
    }

    /// Create a router for HTTP integration if supported
    pub fn create_router(&self) -> Option<axum::Router> {
        use super::transport::{HttpIntegratedTransport, websocket::WebSocketTransport};

        if let Some(ws_transport) = self
            .transport
            .as_any()
            .downcast_ref::<WebSocketTransport<G>>()
        {
            ws_transport.create_router_integration().ok()
        } else {
            None
        }
    }

    /// Parses incoming bytes and routes to appropriate processors
    async fn message_router_loop(
        mut incoming_rx: mpsc::UnboundedReceiver<(NodeId, Bytes)>,
        app_tx: mpsc::UnboundedSender<(NodeId, ApplicationMessage, Option<Uuid>)>,
        raft_tx: mpsc::UnboundedSender<(NodeId, RaftMessage, Option<Uuid>)>,
        cose_handler: Arc<CoseHandler>,
    ) {
        debug!("Message router task started");
        while let Some((sender_id, data)) = incoming_rx.recv().await {
            info!(
                "Message router: Received {} bytes from {}",
                data.len(),
                sender_id
            );
            // Parse and route message
            match Self::parse_incoming_message(&data, &sender_id, &cose_handler).await {
                Ok((message, metadata)) => {
                    info!(
                        "Message router: Parsed message from {} with correlation {:?}",
                        sender_id, metadata.correlation_id
                    );
                    match message {
                        Message::Application(app_msg) => {
                            info!(
                                "Message router: Routing application message from {}",
                                sender_id
                            );
                            if let Err(e) =
                                app_tx.send((sender_id, *app_msg, metadata.correlation_id))
                            {
                                error!("Failed to route application message: {}", e);
                            }
                        }
                        Message::Raft(raft_msg) => {
                            info!("Message router: Routing raft message from {}", sender_id);
                            if let Err(e) =
                                raft_tx.send((sender_id, raft_msg, metadata.correlation_id))
                            {
                                error!("Failed to route raft message: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to parse incoming message from {}: {}", sender_id, e);
                }
            }
        }
        debug!("Message router task exited");
    }

    /// Application Message Processor Task using callback
    async fn application_message_processor_loop(
        mut app_rx: mpsc::UnboundedReceiver<(NodeId, ApplicationMessage, Option<Uuid>)>,
        handler: MessageHandler,
    ) {
        debug!("Application message processor task started");
        while let Some((sender_id, app_message, correlation_id)) = app_rx.recv().await {
            handler(sender_id, app_message, correlation_id);
        }
        debug!("Application message processor task exited");
    }

    /// Raft Message Processor Task using callback
    async fn raft_message_processor_loop(
        mut raft_rx: mpsc::UnboundedReceiver<(NodeId, RaftMessage, Option<Uuid>)>,
        handler: RaftMessageHandler,
    ) {
        debug!("Raft message processor task started");
        while let Some((sender_id, raft_message, correlation_id)) = raft_rx.recv().await {
            handler(sender_id, raft_message, correlation_id);
        }
        debug!("Raft message processor task exited");
    }

    /// Outgoing Message Sender Task
    /// Signs and sends all outgoing messages
    async fn outgoing_message_sender_loop(
        mut outgoing_rx: mpsc::UnboundedReceiver<(NodeId, Message, Option<Uuid>)>,
        transport_tx: mpsc::UnboundedSender<(NodeId, Bytes)>,
        cose_handler: Arc<CoseHandler>,
        _topology: Arc<crate::topology::TopologyManager<G>>,
    ) {
        debug!("Outgoing message sender task started");
        while let Some((target_node_id, message, correlation_id)) = outgoing_rx.recv().await {
            info!(
                "Outgoing message sender: Processing message to {} with correlation {:?}",
                target_node_id, correlation_id
            );
            match Self::sign_message_static(&message, correlation_id, &cose_handler).await {
                Ok(signed_data) => {
                    info!(
                        "Successfully signed message for {}, sending to transport",
                        target_node_id
                    );
                    if let Err(e) = transport_tx.send((target_node_id.clone(), signed_data)) {
                        error!("Failed to send signed message to {}: {}", target_node_id, e);
                    } else {
                        info!("Message queued for transport to {}", target_node_id);
                    }
                }
                Err(e) => {
                    error!("Failed to sign message for {}: {}", target_node_id, e);
                }
            }
        }
        debug!("Outgoing message sender task exited");
    }

    /// Static helper: Parse incoming message and extract metadata
    async fn parse_incoming_message(
        data: &[u8],
        sender_node_id: &NodeId,
        cose_handler: &CoseHandler,
    ) -> Result<(Message, CoseMetadata), Error> {
        // Deserialize and verify COSE message
        let cose_message = cose_handler.deserialize_cose_message(data)?;

        // Verify the message signature and extract metadata
        let (payload_bytes, metadata) = cose_handler
            .verify_signed_message(&cose_message, sender_node_id.clone())
            .await?;

        // Deserialize the actual message
        let message: Message = ciborium::de::from_reader(payload_bytes.as_ref())
            .map_err(|e| Error::InvalidMessage(format!("Failed to deserialize message: {e}")))?;

        Ok((message, metadata))
    }

    /// Static helper: Sign message with optional correlation ID
    async fn sign_message_static(
        message: &Message,
        correlation_id: Option<Uuid>,
        cose_handler: &CoseHandler,
    ) -> Result<Bytes, Error> {
        // Serialize the message using CBOR
        let mut message_data = Vec::new();
        ciborium::ser::into_writer(message, &mut message_data)
            .map_err(|e| Error::InvalidMessage(format!("Failed to serialize message: {e}")))?;

        // Determine message type
        let message_type = match message {
            Message::Application(app_msg) => match app_msg.as_ref() {
                ApplicationMessage::ClusterDiscovery(_) => "cluster_discovery_request",
                ApplicationMessage::ClusterDiscoveryResponse(_) => "cluster_discovery_response",
                ApplicationMessage::ClusterJoinRequest(_) => "cluster_join_request",
                ApplicationMessage::ClusterJoinResponse(_) => "cluster_join_response",
                ApplicationMessage::ConsensusRequest(_) => "consensus_request",
                ApplicationMessage::ConsensusResponse(_) => "consensus_response",
                ApplicationMessage::PubSub(_) => "pubsub",
            },
            Message::Raft(RaftMessage::Vote(_)) => "raft_vote_request",
            Message::Raft(RaftMessage::VoteResponse(_)) => "raft_vote_response",
            Message::Raft(RaftMessage::AppendEntries(_)) => "raft_append_entries_request",
            Message::Raft(RaftMessage::AppendEntriesResponse(_)) => "raft_append_entries_response",
            Message::Raft(RaftMessage::InstallSnapshot(_)) => "raft_install_snapshot_request",
            Message::Raft(RaftMessage::InstallSnapshotResponse(_)) => {
                "raft_install_snapshot_response"
            }
        };

        // Sign with COSE
        let cose_message =
            cose_handler.create_signed_message(&message_data, message_type, correlation_id)?;
        let cose_data = cose_handler.serialize_cose_message(&cose_message)?;

        Ok(Bytes::from(cose_data))
    }
}
