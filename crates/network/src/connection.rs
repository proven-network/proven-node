//! Separate incoming and outgoing connection types with proper state machines

use crate::connection_verifier::{ConnectionState, ConnectionVerifier};
use crate::cose::CoseHandler;
use crate::error::{NetworkError, NetworkResult};
use bytes::Bytes;
use flume;
use proven_attestation::Attestor;
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Connection as TransportConnection;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

/// Type alias for queued messages
type QueuedMessages = Arc<Mutex<Vec<(Bytes, HashMap<String, String>)>>>;

/// A verified message with its sender
#[derive(Debug, Clone)]
pub struct VerifiedMessage {
    pub sender: NodeId,
    pub payload: Bytes,
    pub headers: HashMap<String, String>,
    /// Connection ID this message came from (for bidirectional streams)
    pub connection_id: Option<String>,
}

/// Connection states
#[derive(Debug, Clone)]
enum ConnectionPhase {
    /// Connection is in verification phase
    Verifying,
    /// Connection is verified and ready for normal messages
    Verified { peer_id: NodeId },
}

/// An outgoing connection that initiates verification
pub struct OutgoingConnection<G, A>
where
    G: TopologyAdaptor + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    /// Connection ID
    id: String,
    /// Our node ID (unused but kept for consistency)
    _our_node_id: NodeId,
    /// Expected peer ID (known for outgoing)
    peer_id: NodeId,
    /// Connection phase
    phase: Arc<Mutex<ConnectionPhase>>,
    /// COSE handler for signing/verification
    cose_handler: Arc<CoseHandler>,
    /// Connection verifier
    _verifier: Arc<ConnectionVerifier<G, A>>,
    /// Channel for sending outgoing messages
    outgoing_tx: flume::Sender<Bytes>,
    /// Queued messages to send after verification
    queued_messages: QueuedMessages,
    /// Channel for sending frames
    frame_tx: flume::Sender<crate::message::MultiplexedFrame>,
    /// Handle to the I/O task
    _io_task: JoinHandle<()>,
    /// Handle to the message handler task
    _handler_task: JoinHandle<()>,
}

/// An incoming connection that responds to verification
pub struct IncomingConnection<G, A>
where
    G: TopologyAdaptor + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    /// Connection ID
    id: String,
    /// Our node ID (unused but kept for consistency)
    _our_node_id: NodeId,
    /// Remote peer ID (discovered during verification)
    peer_id: Arc<Mutex<Option<NodeId>>>,
    /// Connection phase
    phase: Arc<Mutex<ConnectionPhase>>,
    /// COSE handler for signing/verification
    cose_handler: Arc<CoseHandler>,
    /// Connection verifier
    _verifier: Arc<ConnectionVerifier<G, A>>,
    /// Channel for sending outgoing messages
    outgoing_tx: flume::Sender<Bytes>,
    /// Queued messages to send after verification
    queued_messages: QueuedMessages,
    /// Channel for sending frames
    frame_tx: flume::Sender<crate::message::MultiplexedFrame>,
    /// Handle to the I/O task
    _io_task: JoinHandle<()>,
    /// Handle to the message handler task
    _handler_task: JoinHandle<()>,
}

impl<G, A> OutgoingConnection<G, A>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Create a new outgoing connection
    pub async fn new(
        id: String,
        _our_node_id: NodeId,
        expected_peer: NodeId,
        transport_conn: Box<dyn TransportConnection>,
        cose_handler: Arc<CoseHandler>,
        verifier: Arc<ConnectionVerifier<G, A>>,
        incoming_tx: flume::Sender<VerifiedMessage>,
    ) -> NetworkResult<Self> {
        // Create channels with proper buffer size
        let (outgoing_tx, outgoing_rx) = flume::bounded(1000);
        let (frame_tx, frame_rx) = flume::bounded(100);

        // Initialize verification
        verifier.initialize_connection(id.clone()).await;

        // Start I/O and handler tasks
        let phase = Arc::new(Mutex::new(ConnectionPhase::Verifying));
        let queued_messages = Arc::new(Mutex::new(Vec::new()));

        let (io_task, handler_task) = Self::start_tasks(
            id.clone(),
            _our_node_id,
            expected_peer,
            phase.clone(),
            queued_messages.clone(),
            transport_conn,
            cose_handler.clone(),
            verifier.clone(),
            outgoing_rx,
            outgoing_tx.clone(),
            incoming_tx,
            frame_rx,
            frame_tx.clone(),
        );

        let conn = Self {
            id: id.clone(),
            _our_node_id,
            peer_id: expected_peer,
            phase,
            cose_handler,
            _verifier: verifier.clone(),
            outgoing_tx,
            queued_messages,
            frame_tx,
            _io_task: io_task,
            _handler_task: handler_task,
        };

        // Send initial verification challenge
        let verification_req = verifier
            .create_verification_request_for_connection(id.clone())
            .await
            .map_err(|e| NetworkError::ConnectionFailed {
                node: Box::new(expected_peer),
                reason: format!("Failed to create verification request: {e}"),
            })?;

        conn.outgoing_tx
            .send_timeout(verification_req, Duration::from_secs(5))
            .map_err(|_| {
                NetworkError::ChannelClosed("Failed to send verification request".to_string())
            })?;

        Ok(conn)
    }

    /// Wait for verification to complete
    pub async fn wait_for_verification(
        &self,
        timeout: std::time::Duration,
    ) -> NetworkResult<NodeId> {
        let start = std::time::Instant::now();

        loop {
            // Check if we're already verified
            if let ConnectionPhase::Verified { peer_id } = &*self.phase.lock().await {
                return Ok(*peer_id);
            }

            if start.elapsed() > timeout {
                return Err(NetworkError::Timeout(timeout));
            }

            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// Send a message on this connection
    pub async fn send(
        &self,
        payload: Bytes,
        headers: HashMap<String, String>,
    ) -> NetworkResult<()> {
        debug!(
            "OutgoingConnection[{}]::send called with headers: {:?}",
            self.id, headers
        );
        // Check if verified
        match &*self.phase.lock().await {
            ConnectionPhase::Verified { peer_id } => {
                debug!(
                    "OutgoingConnection[{}] verified to {}, signing message",
                    self.id, peer_id
                );
                // Sign and send immediately
                let cose_msg = self
                    .cose_handler
                    .create_signed_message(&payload, &headers)
                    .map_err(|e| NetworkError::Serialization(e.to_string()))?;

                let signed_data = self
                    .cose_handler
                    .serialize_cose_message(&cose_msg)
                    .map_err(|e| NetworkError::Serialization(e.to_string()))?;

                debug!(
                    "OutgoingConnection[{}] sending {} bytes to outgoing channel",
                    self.id,
                    signed_data.len()
                );
                self.outgoing_tx
                    .try_send(signed_data)
                    .map_err(|_| NetworkError::ChannelClosed("Connection closed".to_string()))?;
                debug!("OutgoingConnection[{}] message sent successfully", self.id);
            }
            ConnectionPhase::Verifying => {
                debug!(
                    "OutgoingConnection[{}] still verifying, queueing message",
                    self.id
                );
                // Queue message to send after verification
                self.queued_messages.lock().await.push((payload, headers));
            }
        }

        Ok(())
    }

    /// Get the verified peer ID
    pub fn peer_id(&self) -> &NodeId {
        &self.peer_id
    }

    /// Get the connection ID
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Start the I/O and message handler tasks
    #[allow(clippy::too_many_arguments)]
    fn start_tasks(
        conn_id: String,
        _our_node_id: NodeId,
        peer_id: NodeId,
        phase: Arc<Mutex<ConnectionPhase>>,
        queued_messages: QueuedMessages,
        transport_conn: Box<dyn TransportConnection>,
        cose_handler: Arc<CoseHandler>,
        verifier: Arc<ConnectionVerifier<G, A>>,
        outgoing_rx: flume::Receiver<Bytes>,
        outgoing_tx: flume::Sender<Bytes>,
        incoming_tx: flume::Sender<VerifiedMessage>,
        frame_rx: flume::Receiver<crate::message::MultiplexedFrame>,
        _frame_tx: flume::Sender<crate::message::MultiplexedFrame>,
    ) -> (JoinHandle<()>, JoinHandle<()>) {
        // Create channels for I/O coordination with appropriate buffer sizes
        let (send_tx, send_rx) = flume::bounded::<Bytes>(100);
        let (recv_tx, recv_rx) = flume::bounded::<Result<Bytes, String>>(100);

        // I/O task
        let conn = transport_conn;
        let io_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Send outgoing data
                    Ok(data) = send_rx.recv_async() => {
                        debug!("I/O task sending {} bytes to transport", data.len());
                        if let Err(e) = conn.send(data).await {
                            debug!("Send error on connection: {}", e);
                            break;
                        }
                        debug!("I/O task successfully sent data to transport");
                    }
                    // Receive incoming data
                    result = conn.recv() => {
                        match result {
                            Ok(data) => {
                                if recv_tx.try_send(Ok(data)).is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                let _ = recv_tx.try_send(Err(e.to_string()));
                                break;
                            }
                        }
                    }
                }
            }
        });

        // Forward outgoing messages to I/O task
        let send_tx_clone = send_tx.clone();
        tokio::spawn(async move {
            while let Ok(data) = outgoing_rx.recv_async().await {
                debug!(
                    "Forwarding {} bytes from outgoing_rx to I/O task",
                    data.len()
                );
                if send_tx_clone.try_send(data).is_err() {
                    debug!("Failed to forward data to I/O task");
                    break;
                }
            }
        });

        // Forward frames to I/O task
        let send_tx_clone2 = send_tx.clone();
        let cose_handler_clone = cose_handler.clone();
        tokio::spawn(async move {
            while let Ok(frame) = frame_rx.recv_async().await {
                // Serialize frame as a message
                match crate::message::MultiplexedFrame::serialize(&frame) {
                    Ok(frame_bytes) => {
                        // Create headers to indicate this is a frame
                        let mut headers = HashMap::new();
                        headers.insert("message_type".to_string(), "multiplexed_frame".to_string());

                        // Sign the frame
                        match cose_handler_clone.create_signed_message(&frame_bytes, &headers) {
                            Ok(cose_msg) => {
                                match cose_handler_clone.serialize_cose_message(&cose_msg) {
                                    Ok(signed_data) => {
                                        if send_tx_clone2.try_send(signed_data).is_err() {
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to serialize frame COSE message: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to sign frame: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to serialize frame: {}", e);
                    }
                }
            }
        });

        // Message handler task
        let handler_task = tokio::spawn(async move {
            while let Ok(result) = recv_rx.recv_async().await {
                match result {
                    Ok(data) => {
                        if let Err(e) = Self::handle_incoming_message(
                            &conn_id,
                            &peer_id,
                            &phase,
                            &queued_messages,
                            &data,
                            &cose_handler,
                            &verifier,
                            &send_tx,
                            &outgoing_tx,
                            &incoming_tx,
                        )
                        .await
                        {
                            error!("Error handling message on connection {}: {}", conn_id, e);
                            if matches!(e, NetworkError::ConnectionFailed { .. }) {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        debug!("Connection {} closed: {}", conn_id, e);
                        break;
                    }
                }
            }

            // Clean up
            verifier.remove_connection(&conn_id).await;
        });

        (io_task, handler_task)
    }

    /// Handle an incoming message for outgoing connection
    #[allow(clippy::too_many_arguments)]
    async fn handle_incoming_message(
        conn_id: &str,
        peer_id: &NodeId,
        phase: &Arc<Mutex<ConnectionPhase>>,
        queued_messages: &QueuedMessages,
        data: &Bytes,
        cose_handler: &Arc<CoseHandler>,
        verifier: &Arc<ConnectionVerifier<G, A>>,
        send_tx: &flume::Sender<Bytes>,
        outgoing_tx: &flume::Sender<Bytes>,
        incoming_tx: &flume::Sender<VerifiedMessage>,
    ) -> NetworkResult<()> {
        // Check current phase
        let current_phase = phase.lock().await.clone();

        match current_phase {
            ConnectionPhase::Verifying => {
                // In verification phase, only process verification messages
                match verifier
                    .process_verification_message(&conn_id.to_string(), data)
                    .await
                {
                    Ok(Some(response)) => {
                        // Send verification response with timeout
                        if let Err(e) = send_tx.send_timeout(response, Duration::from_secs(5)) {
                            error!("Failed to send verification response: {}", e);
                        }

                        // Check if verification is now complete
                        if let Some(verified_peer) = verifier.get_verified_public_key(conn_id).await
                        {
                            // Verify it's the expected peer
                            if verified_peer != *peer_id {
                                return Err(NetworkError::ConnectionFailed {
                                    node: Box::new(*peer_id),
                                    reason: format!(
                                        "Peer mismatch: expected {peer_id}, got {verified_peer}"
                                    ),
                                });
                            }

                            // Transition to verified state
                            *phase.lock().await = ConnectionPhase::Verified {
                                peer_id: verified_peer,
                            };

                            // Flush queued messages
                            Self::flush_queued_messages(
                                cose_handler,
                                outgoing_tx,
                                queued_messages,
                                conn_id,
                            )
                            .await;
                        }
                    }
                    Ok(None) => {
                        // This shouldn't happen for outgoing connections
                        warn!(
                            "Unexpected None response for outgoing connection {}",
                            conn_id
                        );
                    }
                    Err(e) => {
                        // Check if this is actually a verification failure
                        if let Some(ConnectionState::Failed { .. }) =
                            verifier.get_connection_state(conn_id).await
                        {
                            return Err(NetworkError::ConnectionFailed {
                                node: Box::new(*peer_id),
                                reason: format!("Verification failed: {e}"),
                            });
                        }
                        // Otherwise, it's just not a verification message - drop it
                        warn!(
                            "Dropping non-verification message from unverified connection {}",
                            conn_id
                        );
                    }
                }
            }
            ConnectionPhase::Verified {
                peer_id: verified_peer,
            } => {
                // In verified phase, process regular messages
                let cose_msg = cose_handler.deserialize_cose_message(data).map_err(|e| {
                    NetworkError::Protocol(format!("Failed to deserialize COSE: {e}"))
                })?;

                let (payload, headers) = cose_handler
                    .verify_signed_message(&cose_msg, &verified_peer)
                    .map_err(|e| {
                        NetworkError::Protocol(format!("Signature verification failed: {e}"))
                    })?;

                let msg = VerifiedMessage {
                    sender: verified_peer,
                    payload,
                    headers,
                    connection_id: Some(conn_id.to_string()),
                };

                if incoming_tx.try_send(msg).is_err() {
                    return Err(NetworkError::ChannelClosed(
                        "Incoming channel closed".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Flush queued messages after verification
    async fn flush_queued_messages(
        cose_handler: &Arc<CoseHandler>,
        outgoing_tx: &flume::Sender<Bytes>,
        queued_messages: &QueuedMessages,
        _conn_id: &str,
    ) {
        let messages = {
            let mut queue = queued_messages.lock().await;
            std::mem::take(&mut *queue)
        };

        for (payload, headers) in messages {
            match cose_handler.create_signed_message(&payload, &headers) {
                Ok(cose_msg) => match cose_handler.serialize_cose_message(&cose_msg) {
                    Ok(signed_data) => {
                        if let Err(e) =
                            outgoing_tx.send_timeout(signed_data, Duration::from_millis(100))
                        {
                            error!("Failed to send queued message: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Failed to serialize queued message: {}", e);
                    }
                },
                Err(e) => {
                    error!("Failed to sign queued message: {}", e);
                }
            }
        }
    }
}

impl<G, A> IncomingConnection<G, A>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Create a new incoming connection
    pub async fn new(
        id: String,
        _our_node_id: NodeId,
        transport_conn: Box<dyn TransportConnection>,
        cose_handler: Arc<CoseHandler>,
        verifier: Arc<ConnectionVerifier<G, A>>,
        incoming_tx: flume::Sender<VerifiedMessage>,
    ) -> NetworkResult<Self> {
        // Create channels with proper buffer size
        let (outgoing_tx, outgoing_rx) = flume::bounded(1000);
        let (frame_tx, frame_rx) = flume::bounded(100);

        // Initialize verification
        verifier.initialize_connection(id.clone()).await;

        // Start I/O and handler tasks
        let phase = Arc::new(Mutex::new(ConnectionPhase::Verifying));
        let queued_messages = Arc::new(Mutex::new(Vec::new()));
        let peer_id = Arc::new(Mutex::new(None));

        let (io_task, handler_task) = Self::start_tasks(
            id.clone(),
            _our_node_id,
            peer_id.clone(),
            phase.clone(),
            queued_messages.clone(),
            transport_conn,
            cose_handler.clone(),
            verifier.clone(),
            outgoing_rx,
            outgoing_tx.clone(),
            incoming_tx,
            frame_rx,
            frame_tx.clone(),
        );

        Ok(Self {
            id,
            _our_node_id,
            peer_id,
            phase,
            cose_handler,
            _verifier: verifier,
            outgoing_tx,
            queued_messages,
            frame_tx,
            _io_task: io_task,
            _handler_task: handler_task,
        })
    }

    /// Wait for verification to complete
    pub async fn wait_for_verification(
        &self,
        timeout: std::time::Duration,
    ) -> NetworkResult<NodeId> {
        let start = std::time::Instant::now();

        loop {
            // Check if we're already verified
            if let ConnectionPhase::Verified { peer_id } = &*self.phase.lock().await {
                return Ok(*peer_id);
            }

            if start.elapsed() > timeout {
                return Err(NetworkError::Timeout(timeout));
            }

            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// Send a message on this connection
    pub async fn send(
        &self,
        payload: Bytes,
        headers: HashMap<String, String>,
    ) -> NetworkResult<()> {
        // Check if verified
        match &*self.phase.lock().await {
            ConnectionPhase::Verified { .. } => {
                // Sign and send immediately
                let cose_msg = self
                    .cose_handler
                    .create_signed_message(&payload, &headers)
                    .map_err(|e| NetworkError::Serialization(e.to_string()))?;

                let signed_data = self
                    .cose_handler
                    .serialize_cose_message(&cose_msg)
                    .map_err(|e| NetworkError::Serialization(e.to_string()))?;

                self.outgoing_tx
                    .try_send(signed_data)
                    .map_err(|_| NetworkError::ChannelClosed("Connection closed".to_string()))?;
            }
            ConnectionPhase::Verifying => {
                // Queue message to send after verification
                self.queued_messages.lock().await.push((payload, headers));
            }
        }

        Ok(())
    }

    /// Get the verified peer ID
    pub async fn peer_id(&self) -> Option<NodeId> {
        *self.peer_id.lock().await
    }

    /// Get the connection ID
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Start the I/O and message handler tasks
    #[allow(clippy::too_many_arguments)]
    fn start_tasks(
        conn_id: String,
        _our_node_id: NodeId,
        peer_id: Arc<Mutex<Option<NodeId>>>,
        phase: Arc<Mutex<ConnectionPhase>>,
        queued_messages: QueuedMessages,
        transport_conn: Box<dyn TransportConnection>,
        cose_handler: Arc<CoseHandler>,
        verifier: Arc<ConnectionVerifier<G, A>>,
        outgoing_rx: flume::Receiver<Bytes>,
        outgoing_tx: flume::Sender<Bytes>,
        incoming_tx: flume::Sender<VerifiedMessage>,
        frame_rx: flume::Receiver<crate::message::MultiplexedFrame>,
        _frame_tx: flume::Sender<crate::message::MultiplexedFrame>,
    ) -> (JoinHandle<()>, JoinHandle<()>) {
        // Create channels for I/O coordination with appropriate buffer sizes
        let (send_tx, send_rx) = flume::bounded::<Bytes>(100);
        let (recv_tx, recv_rx) = flume::bounded::<Result<Bytes, String>>(100);

        // I/O task
        let conn = transport_conn;
        let io_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Send outgoing data
                    Ok(data) = send_rx.recv_async() => {
                        debug!("I/O task sending {} bytes to transport", data.len());
                        if let Err(e) = conn.send(data).await {
                            debug!("Send error on connection: {}", e);
                            break;
                        }
                        debug!("I/O task successfully sent data to transport");
                    }
                    // Receive incoming data
                    result = conn.recv() => {
                        match result {
                            Ok(data) => {
                                if recv_tx.try_send(Ok(data)).is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                let _ = recv_tx.try_send(Err(e.to_string()));
                                break;
                            }
                        }
                    }
                }
            }
        });

        // Forward outgoing messages to I/O task
        let send_tx_clone = send_tx.clone();
        tokio::spawn(async move {
            while let Ok(data) = outgoing_rx.recv_async().await {
                debug!(
                    "Forwarding {} bytes from outgoing_rx to I/O task",
                    data.len()
                );
                if send_tx_clone.try_send(data).is_err() {
                    debug!("Failed to forward data to I/O task");
                    break;
                }
            }
        });

        // Forward frames to I/O task
        let send_tx_clone2 = send_tx.clone();
        let cose_handler_clone = cose_handler.clone();
        tokio::spawn(async move {
            while let Ok(frame) = frame_rx.recv_async().await {
                // Serialize frame as a message
                match crate::message::MultiplexedFrame::serialize(&frame) {
                    Ok(frame_bytes) => {
                        // Create headers to indicate this is a frame
                        let mut headers = HashMap::new();
                        headers.insert("message_type".to_string(), "multiplexed_frame".to_string());

                        // Sign the frame
                        match cose_handler_clone.create_signed_message(&frame_bytes, &headers) {
                            Ok(cose_msg) => {
                                match cose_handler_clone.serialize_cose_message(&cose_msg) {
                                    Ok(signed_data) => {
                                        if send_tx_clone2.try_send(signed_data).is_err() {
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to serialize frame COSE message: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to sign frame: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to serialize frame: {}", e);
                    }
                }
            }
        });

        // Message handler task
        let handler_task = tokio::spawn(async move {
            while let Ok(result) = recv_rx.recv_async().await {
                match result {
                    Ok(data) => {
                        if let Err(e) = Self::handle_incoming_message(
                            &conn_id,
                            &peer_id,
                            &phase,
                            &queued_messages,
                            &data,
                            &cose_handler,
                            &verifier,
                            &send_tx,
                            &outgoing_tx,
                            &incoming_tx,
                        )
                        .await
                        {
                            error!("Error handling message on connection {}: {}", conn_id, e);
                            if matches!(e, NetworkError::ConnectionFailed { .. }) {
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        debug!("Connection {} closed: {}", conn_id, e);
                        break;
                    }
                }
            }

            // Clean up
            verifier.remove_connection(&conn_id).await;
        });

        (io_task, handler_task)
    }

    /// Handle an incoming message for incoming connection
    #[allow(clippy::too_many_arguments)]
    async fn handle_incoming_message(
        conn_id: &str,
        peer_id: &Arc<Mutex<Option<NodeId>>>,
        phase: &Arc<Mutex<ConnectionPhase>>,
        queued_messages: &QueuedMessages,
        data: &Bytes,
        cose_handler: &Arc<CoseHandler>,
        verifier: &Arc<ConnectionVerifier<G, A>>,
        send_tx: &flume::Sender<Bytes>,
        outgoing_tx: &flume::Sender<Bytes>,
        incoming_tx: &flume::Sender<VerifiedMessage>,
    ) -> NetworkResult<()> {
        // Check current phase
        let current_phase = phase.lock().await.clone();

        match current_phase {
            ConnectionPhase::Verifying => {
                // In verification phase, only process verification messages
                match verifier
                    .process_verification_message(&conn_id.to_string(), data)
                    .await
                {
                    Ok(Some(response)) => {
                        // Send verification response with timeout
                        if let Err(e) = send_tx.send_timeout(response, Duration::from_secs(5)) {
                            error!("Failed to send verification response: {}", e);
                        }
                    }
                    Ok(None) => {
                        // Verification complete - check if we're now verified
                        if let Some(verified_peer) = verifier.get_verified_public_key(conn_id).await
                        {
                            // Transition to verified state
                            *phase.lock().await = ConnectionPhase::Verified {
                                peer_id: verified_peer,
                            };

                            // Update peer_id
                            *peer_id.lock().await = Some(verified_peer);

                            // Flush queued messages
                            Self::flush_queued_messages(
                                cose_handler,
                                outgoing_tx,
                                queued_messages,
                                conn_id,
                            )
                            .await;
                        }
                    }
                    Err(e) => {
                        // Check if this is actually a verification failure
                        if let Some(ConnectionState::Failed { .. }) =
                            verifier.get_connection_state(conn_id).await
                        {
                            return Err(NetworkError::ConnectionFailed {
                                node: Box::new((*peer_id.lock().await).unwrap_or_default()),
                                reason: format!("Verification failed: {e}"),
                            });
                        }
                        // Otherwise, it's just not a verification message - drop it
                        warn!(
                            "Dropping non-verification message from unverified connection {}",
                            conn_id
                        );
                    }
                }
            }
            ConnectionPhase::Verified {
                peer_id: verified_peer,
            } => {
                // In verified phase, process regular messages
                let cose_msg = cose_handler.deserialize_cose_message(data).map_err(|e| {
                    NetworkError::Protocol(format!("Failed to deserialize COSE: {e}"))
                })?;

                let (payload, headers) = cose_handler
                    .verify_signed_message(&cose_msg, &verified_peer)
                    .map_err(|e| {
                        NetworkError::Protocol(format!("Signature verification failed: {e}"))
                    })?;

                let msg = VerifiedMessage {
                    sender: verified_peer,
                    payload,
                    headers,
                    connection_id: Some(conn_id.to_string()),
                };

                if incoming_tx.try_send(msg).is_err() {
                    return Err(NetworkError::ChannelClosed(
                        "Incoming channel closed".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Flush queued messages after verification
    async fn flush_queued_messages(
        cose_handler: &Arc<CoseHandler>,
        outgoing_tx: &flume::Sender<Bytes>,
        queued_messages: &QueuedMessages,
        _conn_id: &str,
    ) {
        let messages = {
            let mut queue = queued_messages.lock().await;
            std::mem::take(&mut *queue)
        };

        for (payload, headers) in messages {
            match cose_handler.create_signed_message(&payload, &headers) {
                Ok(cose_msg) => match cose_handler.serialize_cose_message(&cose_msg) {
                    Ok(signed_data) => {
                        if let Err(e) =
                            outgoing_tx.send_timeout(signed_data, Duration::from_millis(100))
                        {
                            error!("Failed to send queued message: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Failed to serialize queued message: {}", e);
                    }
                },
                Err(e) => {
                    error!("Failed to sign queued message: {}", e);
                }
            }
        }
    }
}

/// A connection that can be either incoming or outgoing
pub enum Connection<G, A>
where
    G: TopologyAdaptor + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    Incoming(IncomingConnection<G, A>),
    Outgoing(OutgoingConnection<G, A>),
}

impl<G, A> Connection<G, A>
where
    G: TopologyAdaptor + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Check if the connection is still healthy (can send messages)
    pub fn is_healthy(&self) -> bool {
        match self {
            Connection::Outgoing(c) => !c.outgoing_tx.is_disconnected(),
            Connection::Incoming(c) => !c.outgoing_tx.is_disconnected(),
        }
    }

    /// Create a new outgoing connection
    pub async fn new_outgoing(
        id: String,
        _our_node_id: NodeId,
        expected_peer: NodeId,
        transport_conn: Box<dyn TransportConnection>,
        cose_handler: Arc<CoseHandler>,
        verifier: Arc<ConnectionVerifier<G, A>>,
        incoming_tx: flume::Sender<VerifiedMessage>,
    ) -> NetworkResult<Self> {
        let conn = OutgoingConnection::new(
            id,
            _our_node_id,
            expected_peer,
            transport_conn,
            cose_handler,
            verifier,
            incoming_tx,
        )
        .await?;
        Ok(Connection::Outgoing(conn))
    }

    /// Create a new incoming connection
    pub async fn new_incoming(
        id: String,
        _our_node_id: NodeId,
        transport_conn: Box<dyn TransportConnection>,
        cose_handler: Arc<CoseHandler>,
        verifier: Arc<ConnectionVerifier<G, A>>,
        incoming_tx: flume::Sender<VerifiedMessage>,
    ) -> NetworkResult<Self> {
        let conn = IncomingConnection::new(
            id,
            _our_node_id,
            transport_conn,
            cose_handler,
            verifier,
            incoming_tx,
        )
        .await?;
        Ok(Connection::Incoming(conn))
    }

    /// Wait for verification to complete
    pub async fn wait_for_verification(
        &self,
        timeout: std::time::Duration,
    ) -> NetworkResult<NodeId> {
        match self {
            Connection::Incoming(conn) => conn.wait_for_verification(timeout).await,
            Connection::Outgoing(conn) => conn.wait_for_verification(timeout).await,
        }
    }

    /// Send a message on this connection
    pub async fn send(
        &self,
        payload: Bytes,
        headers: HashMap<String, String>,
    ) -> NetworkResult<()> {
        match self {
            Connection::Incoming(conn) => conn.send(payload, headers).await,
            Connection::Outgoing(conn) => conn.send(payload, headers).await,
        }
    }

    /// Get the connection ID
    pub fn id(&self) -> &str {
        match self {
            Connection::Incoming(conn) => conn.id(),
            Connection::Outgoing(conn) => conn.id(),
        }
    }

    /// Get the peer ID
    pub async fn peer_id(&self) -> Option<NodeId> {
        match self {
            Connection::Incoming(conn) => conn.peer_id().await,
            Connection::Outgoing(conn) => Some(*conn.peer_id()),
        }
    }

    /// Get frame sender for multiplexing
    pub fn get_frame_sender(&self) -> flume::Sender<crate::message::MultiplexedFrame> {
        match self {
            Connection::Incoming(conn) => conn.frame_tx.clone(),
            Connection::Outgoing(conn) => conn.frame_tx.clone(),
        }
    }

    /// Send a frame on this connection
    pub async fn send_frame(&self, frame: crate::message::MultiplexedFrame) -> NetworkResult<()> {
        let frame_tx = self.get_frame_sender();
        frame_tx
            .send_async(frame)
            .await
            .map_err(|_| NetworkError::ChannelClosed("Frame channel closed".to_string()))?;
        Ok(())
    }
}
