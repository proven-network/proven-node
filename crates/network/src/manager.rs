//! Network manager - the main entry point for the streaming network layer

use crate::connection::VerifiedMessage;
use crate::connection_pool::{ConnectionPool, ConnectionPoolConfig};
use crate::error::{NetworkError, NetworkResult};
use crate::message::{NetworkMessage, ServiceMessage};
use crate::service::{ServiceContext, ServiceHandler, StreamingServiceHandler};
use dashmap::DashMap;
use ed25519_dalek::SigningKey;
use flume;
use proven_topology::{NodeId, TopologyAdaptor, TopologyManager};
use proven_transport::Transport;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Main network manager with generic transport, topology and attestor
pub struct NetworkManager<T, G, A>
where
    T: Transport,
    G: TopologyAdaptor,
    A: proven_attestation::Attestor,
{
    /// Local node ID
    _local_node_id: NodeId,
    /// Connection pool for managing connections
    connection_pool: Arc<ConnectionPool<T, G, A>>,
    /// Topology manager
    topology: Arc<TopologyManager<G>>,
    /// Service handlers
    service_handlers: Arc<DashMap<&'static str, Arc<dyn ServiceHandler>>>,
    /// Streaming service handlers
    streaming_handlers: Arc<DashMap<&'static str, Arc<dyn StreamingServiceHandler>>>,
    /// Active streams
    streams: Arc<DashMap<Uuid, StreamInfo>>,
    /// Pending responses for request/response pattern
    pending_responses: Arc<DashMap<Uuid, oneshot::Sender<bytes::Bytes>>>,
    /// Incoming message handler task
    message_handler: RwLock<Option<JoinHandle<()>>>,
    /// Listener task
    listener_task: RwLock<Option<JoinHandle<()>>>,
    /// Channel for verified incoming messages
    _incoming_tx: flume::Sender<VerifiedMessage>,
    incoming_rx: Arc<tokio::sync::Mutex<flume::Receiver<VerifiedMessage>>>,
    /// Shutdown signal
    shutdown: Arc<tokio::sync::Notify>,
    /// Channel for incoming streams
    incoming_streams_tx: flume::Sender<IncomingStream>,
    incoming_streams_rx: flume::Receiver<IncomingStream>,
}

/// An incoming stream from a remote peer
pub struct IncomingStream {
    pub stream: crate::stream::Stream,
    pub metadata: HashMap<String, String>,
}

/// Information about an active stream
struct StreamInfo {
    _stream_type: String,
    _peer: NodeId,
    sender: flume::Sender<bytes::Bytes>,
}

impl<T, G, A> NetworkManager<T, G, A>
where
    T: Transport + Send + Sync + 'static,
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Create a new network manager
    pub fn new(
        local_node_id: NodeId,
        transport: Arc<T>,
        topology: Arc<TopologyManager<G>>,
        signing_key: SigningKey,
        config: ConnectionPoolConfig,
        governance: Arc<G>,
        attestor: Arc<A>,
    ) -> Self {
        let (incoming_tx, incoming_rx) = flume::bounded(10000);
        let (incoming_streams_tx, incoming_streams_rx) = flume::bounded(100);

        let connection_pool = Arc::new(ConnectionPool::new(
            transport,
            local_node_id.clone(),
            signing_key,
            config,
            incoming_tx.clone(),
            governance,
            attestor,
        ));

        Self {
            _local_node_id: local_node_id,
            connection_pool,
            topology,
            service_handlers: Arc::new(DashMap::new()),
            streaming_handlers: Arc::new(DashMap::new()),
            streams: Arc::new(DashMap::new()),
            pending_responses: Arc::new(DashMap::new()),
            message_handler: RwLock::new(None),
            listener_task: RwLock::new(None),
            _incoming_tx: incoming_tx,
            incoming_rx: Arc::new(tokio::sync::Mutex::new(incoming_rx)),
            shutdown: Arc::new(tokio::sync::Notify::new()),
            incoming_streams_tx,
            incoming_streams_rx,
        }
    }

    /// Start the network manager
    pub async fn start(&self) -> NetworkResult<()> {
        // Start listener
        let transport = self.connection_pool.transport.clone();
        let listener = transport
            .listen()
            .await
            .map_err(|e| NetworkError::Transport(e.to_string()))?;

        info!("Network manager started");

        // Spawn listener task
        let pool = self.connection_pool.clone();
        let shutdown = self.shutdown.clone();
        let listener_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    conn = listener.accept() => {
                        match conn {
                            Ok(conn) => {
                                pool.handle_incoming(conn).await;
                            }
                            Err(e) => {
                                error!("Failed to accept connection: {}", e);
                            }
                        }
                    }
                    _ = shutdown.notified() => {
                        info!("Listener shutting down");
                        break;
                    }
                }
            }
        });

        *self.listener_task.write().await = Some(listener_task);

        // Start message handler
        let handlers = self.service_handlers.clone();
        let streaming_handlers = self.streaming_handlers.clone();
        let streams = self.streams.clone();
        let pending_responses = self.pending_responses.clone();
        let connection_pool = self.connection_pool.clone();
        let topology = self.topology.clone();
        let incoming_rx = self.incoming_rx.clone();
        let shutdown = self.shutdown.clone();
        let incoming_streams_tx = self.incoming_streams_tx.clone();

        let task = tokio::spawn(async move {
            let rx = incoming_rx.lock().await;
            loop {
                tokio::select! {
                    msg = rx.recv_async() => {
                        match msg {
                            Ok(verified_msg) => {
                                Self::handle_message(
                                    verified_msg,
                                    &handlers,
                                    &streaming_handlers,
                                    &streams,
                                    &pending_responses,
                                    &connection_pool,
                                    &topology,
                                    &incoming_streams_tx,
                                ).await;
                            }
                            Err(_) => break, // Channel closed
                        }
                    }
                    _ = shutdown.notified() => {
                        info!("Message handler shutting down");
                        break;
                    }
                }
            }
        });

        *self.message_handler.write().await = Some(task);
        Ok(())
    }

    /// Handle an incoming message
    #[allow(clippy::too_many_arguments)]
    async fn handle_message(
        msg: VerifiedMessage,
        handlers: &Arc<DashMap<&'static str, Arc<dyn ServiceHandler>>>,
        streaming_handlers: &Arc<DashMap<&'static str, Arc<dyn StreamingServiceHandler>>>,
        streams: &Arc<DashMap<Uuid, StreamInfo>>,
        pending_responses: &Arc<DashMap<Uuid, oneshot::Sender<bytes::Bytes>>>,
        connection_pool: &Arc<ConnectionPool<T, G, A>>,
        topology: &Arc<TopologyManager<G>>,
        incoming_streams_tx: &flume::Sender<IncomingStream>,
    ) {
        debug!(
            "Handling message from {} with headers: {:?}",
            msg.sender, msg.headers
        );
        // Check if this is a multiplexed frame
        if msg.headers.get("message_type").map(|s| s.as_str()) == Some("multiplexed_frame") {
            // Deserialize the frame
            if let Ok(frame) = crate::message::MultiplexedFrame::deserialize(&msg.payload) {
                match frame.data {
                    crate::message::FrameData::Stream(stream_frame) => {
                        match stream_frame {
                            crate::stream::StreamFrame::Open {
                                stream_id,
                                stream_type,
                                metadata,
                            } => {
                                // Create receiver side of stream
                                let (tx, rx) = flume::bounded::<bytes::Bytes>(100);
                                let flow_control =
                                    Arc::new(crate::stream::FlowController::new(65536));

                                // Get the connection's frame sender for bidirectional communication
                                let frame_sender = if let Some(conn_id) = &msg.connection_id {
                                    connection_pool
                                        .get_connection_by_id(conn_id)
                                        .map(|conn| conn.get_frame_sender())
                                } else {
                                    None
                                };

                                let handle = crate::stream::StreamHandle {
                                    id: stream_id,
                                    peer: msg.sender.clone(),
                                    stream_type: stream_type.clone(),
                                    sender: if let Some(frame_tx) = frame_sender {
                                        crate::stream::StreamSender::new_with_frame_sender(
                                            tx.clone(),
                                            flow_control.clone(),
                                            stream_id,
                                            frame_tx,
                                        )
                                    } else {
                                        crate::stream::StreamSender::new(
                                            tx.clone(),
                                            flow_control.clone(),
                                        )
                                    },
                                    receiver: crate::stream::StreamReceiver::new(
                                        rx,
                                        flow_control.clone(),
                                    ),
                                    flow_control,
                                };

                                streams.insert(
                                    stream_id,
                                    StreamInfo {
                                        _stream_type: stream_type.clone(),
                                        _peer: msg.sender.clone(),
                                        sender: tx,
                                    },
                                );

                                // Send to incoming streams channel or handler
                                if let Some(handler) = streaming_handlers.get(stream_type.as_str())
                                {
                                    let stream = crate::stream::Stream { handle };
                                    let handler = handler.clone();
                                    let sender = msg.sender.clone();
                                    tokio::spawn(async move {
                                        if let Err(e) =
                                            handler.handle_stream(sender, stream, metadata).await
                                        {
                                            error!("Stream handler error: {}", e);
                                        }
                                    });
                                } else {
                                    // Send to incoming streams channel
                                    let _ = incoming_streams_tx.try_send(IncomingStream {
                                        stream: crate::stream::Stream { handle },
                                        metadata,
                                    });
                                }
                            }
                            crate::stream::StreamFrame::Data {
                                stream_id, payload, ..
                            } => {
                                // Route to existing stream
                                if let Some(stream_info) = streams.get(&stream_id) {
                                    let _ = stream_info.sender.try_send(payload);
                                }
                            }
                            crate::stream::StreamFrame::WindowUpdate {
                                stream_id,
                                increment,
                            } => {
                                // Update flow control for stream
                                if let Some(_stream_info) = streams.get(&stream_id) {
                                    // TODO: Need to access the stream's flow controller
                                    // For now, just log that we received it
                                    debug!(
                                        "Received window update for stream {}: increment {}",
                                        stream_id, increment
                                    );
                                }
                            }
                            crate::stream::StreamFrame::Reset { stream_id, .. } => {
                                // Close stream
                                streams.remove(&stream_id);
                            }
                        }
                    }
                    _ => {
                        // Other frame types not yet supported
                        warn!("Unsupported frame type");
                    }
                }
                return;
            }
        }
        // Check if this is a response to a pending request
        if msg.headers.get("is_response").map(|s| s.as_str()) == Some("true") {
            if let Some(stream_id_str) = msg.headers.get("stream_id") {
                if let Ok(stream_id) = Uuid::parse_str(stream_id_str) {
                    if let Some((_, response_sender)) = pending_responses.remove(&stream_id) {
                        debug!("Routing response for stream_id: {}", stream_id);
                        let _ = response_sender.send(msg.payload);
                        return;
                    } else {
                        warn!("Received response for unknown stream_id: {}", stream_id);
                    }
                }
            }
        }

        // Check if this is for an existing stream
        if let Some(stream_id_str) = msg.headers.get("stream_id") {
            if let Ok(stream_id) = Uuid::parse_str(stream_id_str) {
                // Check if this is for an active stream
                if let Some(stream_info) = streams.get(&stream_id) {
                    // Forward to stream
                    let _ = stream_info.sender.try_send(msg.payload);
                    return;
                }
            }
        }

        // Check if this is a new stream request
        if let Some(stream_type) = msg.headers.get("stream_type") {
            debug!("Received message with stream_type: {}", stream_type);
            if let Some(service_id) = stream_type.strip_prefix("req_res_") {
                debug!("Handling request/response for service: {}", service_id);
                // Handle request/response
                if let Some(handler) = handlers.get(service_id) {
                    debug!("Found handler for service: {}", service_id);
                    let ctx = ServiceContext {
                        sender: msg.sender.clone(),
                        correlation_id: msg
                            .headers
                            .get("stream_id")
                            .and_then(|s| Uuid::parse_str(s).ok()),
                    };

                    let handler = handler.clone();
                    let connection_pool = connection_pool.clone();
                    let topology = topology.clone();
                    let sender = msg.sender.clone();
                    let correlation_id = ctx.correlation_id;

                    tokio::spawn(async move {
                        debug!(
                            "Handler task spawned for correlation_id: {:?}",
                            correlation_id
                        );
                        match handler.handle(msg.payload, ctx).await {
                            Ok(response_bytes) => {
                                debug!(
                                    "Handler returned response, sending back with correlation_id: {:?}",
                                    correlation_id
                                );
                                // Send response back
                                if let Some(stream_id) = correlation_id {
                                    // Get peer address from topology
                                    if let Some(peer) = topology.get_peer_by_node_id(&sender).await
                                    {
                                        let mut headers = HashMap::new();
                                        headers
                                            .insert("stream_id".to_string(), stream_id.to_string());
                                        headers
                                            .insert("is_response".to_string(), "true".to_string());

                                        if let Err(e) = connection_pool
                                            .send(&peer, response_bytes, headers)
                                            .await
                                        {
                                            warn!("Failed to send response: {}", e);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Handler error: {}", e);
                            }
                        }
                    });
                }
            } else if let Some(_handler) = streaming_handlers.get(stream_type.as_str()) {
                // Handle streaming service
                // TODO: Create stream and invoke handler
            }
        }
    }

    /// Send a request and wait for response
    pub async fn request_with_timeout<M: ServiceMessage>(
        &self,
        target: NodeId,
        message: M,
        timeout: Duration,
    ) -> NetworkResult<M::Response> {
        debug!(
            "Sending request to {} with timeout {:?}, service: {}",
            target,
            timeout,
            M::service_id()
        );

        // Get peer address from topology
        let peer = self
            .topology
            .get_peer_by_node_id(&target)
            .await
            .ok_or_else(|| {
                warn!(
                    "Peer {} not found in topology for service {}",
                    target,
                    M::service_id()
                );
                NetworkError::PeerNotFound(Box::new(target.clone()))
            })?;

        debug!("Found peer {} at {}", target, peer.origin);

        // Create temporary stream ID for correlation
        let stream_id = Uuid::new_v4();
        let mut headers = HashMap::new();
        headers.insert(
            "stream_type".to_string(),
            format!("req_res_{}", M::service_id()),
        );
        headers.insert("stream_id".to_string(), stream_id.to_string());

        // Create response channel
        let (response_tx, response_rx) = oneshot::channel();

        // Register pending response
        self.pending_responses.insert(stream_id, response_tx);

        // Serialize message
        let payload = NetworkMessage::serialize(&message)?;

        // Send request
        debug!(
            "Sending {} request to {} (stream_id: {})",
            M::service_id(),
            peer.origin,
            stream_id
        );
        debug!("About to send request via connection pool");
        self.connection_pool.send(&peer, payload, headers).await?;
        debug!("Request sent successfully via connection pool, waiting for response");

        // Wait for response with timeout
        let result = tokio::time::timeout(timeout, async {
            match response_rx.await {
                Ok(response_bytes) => {
                    debug!("Received response for stream_id: {}", stream_id);
                    // Deserialize response
                    M::Response::deserialize(&response_bytes)
                }
                Err(_) => {
                    warn!("Response channel closed for stream_id: {}", stream_id);
                    Err(NetworkError::ChannelClosed(
                        "Response channel closed".to_string(),
                    ))
                }
            }
        })
        .await
        .unwrap_or_else(|_| {
            // Clean up pending response on timeout
            self.pending_responses.remove(&stream_id);
            warn!(
                "Request to {} timed out after {:?} (stream_id: {})",
                target, timeout, stream_id
            );
            Err(NetworkError::Timeout(timeout))
        });

        match &result {
            Ok(_) => debug!("Request to {} completed successfully", target),
            Err(e) => warn!("Request to {} failed: {}", target, e),
        }

        result
    }

    /// Register a request-response service
    pub async fn register_service<S: crate::service::Service>(
        &self,
        service: S,
    ) -> NetworkResult<()> {
        let handler = crate::service::create_service_handler(service);
        let service_id = handler.service_id();

        if self.service_handlers.contains_key(service_id) {
            return Err(NetworkError::HandlerAlreadyRegistered {
                service: service_id.to_string(),
            });
        }

        self.service_handlers.insert(service_id, Arc::from(handler));
        Ok(())
    }

    /// Open a stream to a remote peer
    pub async fn open_stream(
        &self,
        target: NodeId,
        stream_type: &str,
        metadata: HashMap<String, String>,
    ) -> NetworkResult<crate::stream::Stream> {
        // Get peer address from topology
        let peer = self
            .topology
            .get_peer_by_node_id(&target)
            .await
            .ok_or_else(|| NetworkError::PeerNotFound(Box::new(target.clone())))?;

        // Get or create connection
        let conn = self.connection_pool.get_connection(&peer).await?;

        // Generate stream ID
        let stream_id = Uuid::new_v4();

        // Create local stream channels
        let (tx, rx) = flume::bounded::<bytes::Bytes>(100);
        let flow_control = Arc::new(crate::stream::FlowController::new(65536)); // 64KB window

        // Create stream handle with frame sender
        let frame_tx = conn.get_frame_sender();
        let handle = crate::stream::StreamHandle {
            id: stream_id,
            peer: target.clone(),
            stream_type: stream_type.to_string(),
            sender: crate::stream::StreamSender::new_with_frame_sender(
                tx.clone(),
                flow_control.clone(),
                stream_id,
                frame_tx,
            ),
            receiver: crate::stream::StreamReceiver::new(rx, flow_control.clone()),
            flow_control,
        };

        // Send StreamOpen frame
        let open_frame = crate::message::MultiplexedFrame {
            stream_id: Some(stream_id),
            data: crate::message::FrameData::Stream(crate::stream::StreamFrame::Open {
                stream_id,
                stream_type: stream_type.to_string(),
                metadata,
            }),
        };

        conn.send_frame(open_frame).await?;

        // Register stream
        self.streams.insert(
            stream_id,
            StreamInfo {
                _stream_type: stream_type.to_string(),
                _peer: target,
                sender: tx,
            },
        );

        Ok(crate::stream::Stream { handle })
    }

    /// Get a receiver for incoming streams
    pub fn incoming_streams(&self) -> flume::Receiver<IncomingStream> {
        self.incoming_streams_rx.clone()
    }

    /// Register a streaming service handler
    pub async fn register_streaming_service<S>(&self, service: S) -> NetworkResult<()>
    where
        S: crate::service::StreamingService + 'static,
    {
        let handler = crate::service::create_streaming_handler(service);
        let stream_type = handler.stream_type();

        if self.streaming_handlers.contains_key(stream_type) {
            return Err(NetworkError::HandlerAlreadyRegistered {
                service: stream_type.to_string(),
            });
        }

        self.streaming_handlers
            .insert(stream_type, Arc::from(handler));
        Ok(())
    }

    /// Stop the network manager
    pub async fn stop(&self) -> NetworkResult<()> {
        // Signal shutdown
        self.shutdown.notify_waiters();

        // Wait for tasks to stop
        if let Some(task) = self.message_handler.write().await.take() {
            let _ = task.await;
        }

        if let Some(task) = self.listener_task.write().await.take() {
            let _ = task.await;
        }

        Ok(())
    }
}
