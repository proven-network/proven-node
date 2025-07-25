//! Simplified client service implementation
//!
//! The ClientService acts as a thin coordinator, delegating requests to specialized handlers.

use std::sync::Arc;

use proven_network::NetworkManager;
use proven_storage::{LogIndex, StorageAdaptor};
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;
use tokio::sync::{RwLock, mpsc, oneshot};
use tracing::{debug, info};

use crate::{
    error::{ConsensusResult, Error},
    foundation::{
        events::EventBus, routing::RoutingTable, traits::ServiceLifecycle, types::ConsensusGroupId,
    },
    services::{
        global_consensus::GlobalConsensusService, group_consensus::GroupConsensusService,
        stream::StreamService,
    },
};

use super::{
    handlers::{
        GlobalHandler, GroupHandler, QueryHandler, StreamHandler, StreamReadHandler,
        StreamStreamingHandler,
    },
    network::RequestForwarder,
    types::*,
};

// Type aliases to simplify complex types
type ServiceRef<T> = Arc<RwLock<Option<Arc<T>>>>;
type GlobalConsensusRef<T, G, S> = ServiceRef<GlobalConsensusService<T, G, S>>;
type GroupConsensusRef<T, G, S> = ServiceRef<GroupConsensusService<T, G, S>>;
type StreamServiceRef<S> = ServiceRef<StreamService<S>>;
type NetworkManagerRef<T, G> = ServiceRef<NetworkManager<T, G>>;
type ClientHandlersRef<T, G, S> = Arc<RwLock<Option<Arc<ClientHandlers<T, G, S>>>>>;

/// Client handlers container
struct ClientHandlers<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    global: GlobalHandler<T, G, S>,
    group: GroupHandler<T, G, S>,
    stream: StreamHandler<T, G, S>,
    stream_read: StreamReadHandler<S>,
    stream_streaming: Option<StreamStreamingHandler<S>>,
    query: QueryHandler<T, G, S>,
}

/// Client service for handling client requests
pub struct ClientService<T, G, S>
where
    T: Transport,
    G: TopologyAdaptor,
    S: StorageAdaptor,
{
    /// Service name
    name: String,

    /// Node ID
    node_id: NodeId,

    /// Request receiver
    request_rx: Arc<RwLock<Option<mpsc::Receiver<ClientRequest>>>>,

    /// Request sender (for submitting requests)
    request_tx: mpsc::Sender<ClientRequest>,

    /// Handlers
    handlers: ClientHandlersRef<T, G, S>,

    /// Request forwarder
    forwarder: Arc<RequestForwarder<T, G>>,

    /// Service references (for initialization)
    global_consensus: GlobalConsensusRef<T, G, S>,
    group_consensus: GroupConsensusRef<T, G, S>,
    stream_service: StreamServiceRef<S>,
    network_manager: NetworkManagerRef<T, G>,

    /// Event bus for publishing events
    event_bus: Arc<EventBus>,

    /// Routing table
    routing_table: Arc<RoutingTable>,

    /// Whether the service is running
    is_running: Arc<RwLock<bool>>,

    /// Service shutdown signal
    shutdown: Arc<RwLock<Option<oneshot::Sender<()>>>>,
}

impl<T, G, S> ClientService<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    /// Create a new client service
    pub fn new(
        node_id: NodeId,
        event_bus: Arc<EventBus>,
        routing_table: Arc<RoutingTable>,
    ) -> Self {
        let (request_tx, request_rx) = mpsc::channel(1000);

        // Service references
        let global_consensus = Arc::new(RwLock::new(None));
        let group_consensus = Arc::new(RwLock::new(None));
        let stream_service = Arc::new(RwLock::new(None));
        let network_manager = Arc::new(RwLock::new(None));

        // Create forwarder
        let forwarder = Arc::new(RequestForwarder::new(
            node_id.clone(),
            network_manager.clone(),
            event_bus.clone(),
            routing_table.clone(),
        ));

        Self {
            name: "ClientService".to_string(),
            node_id,
            request_rx: Arc::new(RwLock::new(Some(request_rx))),
            request_tx,
            handlers: Arc::new(RwLock::new(None)),
            forwarder,
            global_consensus,
            group_consensus,
            stream_service,
            network_manager,
            event_bus,
            routing_table,
            is_running: Arc::new(RwLock::new(false)),
            shutdown: Arc::new(RwLock::new(None)),
        }
    }

    /// Set the global consensus service reference
    pub async fn set_global_consensus(&self, service: Arc<GlobalConsensusService<T, G, S>>) {
        *self.global_consensus.write().await = Some(service);
    }

    /// Set the group consensus service reference
    pub async fn set_group_consensus(&self, service: Arc<GroupConsensusService<T, G, S>>) {
        *self.group_consensus.write().await = Some(service);
    }

    /// Set the stream service reference
    pub async fn set_stream_service(&self, service: Arc<StreamService<S>>) {
        *self.stream_service.write().await = Some(service);
    }

    /// Set the network manager reference
    pub async fn set_network_manager(&self, network: Arc<NetworkManager<T, G>>) {
        *self.network_manager.write().await = Some(network);
    }

    /// Get a sender for submitting client requests
    pub fn get_request_sender(&self) -> mpsc::Sender<ClientRequest> {
        self.request_tx.clone()
    }

    /// Get a suitable group for stream creation
    pub async fn get_suitable_group(&self) -> ConsensusResult<ConsensusGroupId> {
        // Use routing table to find least loaded group
        self.routing_table
            .find_least_loaded_group()
            .await
            .ok_or_else(|| {
                Error::with_context(crate::error::ErrorKind::NotFound, "No groups available")
            })
    }

    /// Read messages directly from a stream
    pub async fn read_stream(
        &self,
        stream_name: &str,
        start_sequence: LogIndex,
        count: LogIndex,
    ) -> ConsensusResult<Vec<crate::services::stream::StoredMessage>> {
        let route_info = self
            .routing_table
            .get_stream_route(stream_name)
            .await
            .map_err(|e| {
                Error::with_context(
                    crate::error::ErrorKind::Service,
                    format!("Failed to get routing info: {e}"),
                )
            })?
            .ok_or_else(|| {
                Error::with_context(
                    crate::error::ErrorKind::NotFound,
                    format!("Stream '{stream_name}' not found"),
                )
            })?;

        let is_local = self
            .routing_table
            .is_group_local(route_info.group_id)
            .await
            .unwrap_or(false);

        if is_local {
            // Stream is local - use event bus to read messages
            use crate::services::stream::StreamName;
            use crate::services::stream::commands::ReadMessages;

            Ok(self
                .event_bus
                .request(ReadMessages {
                    stream_name: StreamName::new(stream_name),
                    start_offset: start_sequence.get(),
                    count: count.get(),
                })
                .await
                .map_err(|e| {
                    Error::with_context(
                        crate::error::ErrorKind::Service,
                        format!("Failed to read messages: {e}"),
                    )
                })?)
        } else {
            // Stream is remote
            self.forwarder
                .forward_read_request(route_info.group_id, stream_name, start_sequence, count)
                .await
        }
    }

    /// Initialize handlers after all services are set
    async fn initialize_handlers(&self) -> ConsensusResult<()> {
        // Create handlers
        let global_handler = GlobalHandler::new(
            self.node_id.clone(),
            self.network_manager.clone(),
            self.event_bus.clone(),
        );

        let group_handler = GroupHandler::new(
            self.event_bus.clone(),
            self.forwarder.clone(),
            self.routing_table.clone(),
        );

        let stream_handler = StreamHandler::new(
            self.event_bus.clone(),
            Arc::new(group_handler.clone()),
            self.routing_table.clone(),
        );

        let stream_read_handler = StreamReadHandler::new(self.stream_service.clone());

        // Create streaming handler - StorageAdaptor now requires LogStorageStreaming
        let stream_streaming = Some(StreamStreamingHandler::new(self.stream_service.clone()));

        let query_handler = QueryHandler::new(
            self.event_bus.clone(),
            self.forwarder.clone(),
            self.routing_table.clone(),
        );

        let handlers = Arc::new(ClientHandlers {
            global: global_handler,
            group: group_handler,
            stream: stream_handler,
            stream_read: stream_read_handler,
            stream_streaming,
            query: query_handler,
        });

        *self.handlers.write().await = Some(handlers);
        Ok(())
    }

    /// Process incoming client requests
    async fn process_requests(&self) {
        let mut request_rx = match self.request_rx.write().await.take() {
            Some(rx) => rx,
            None => {
                tracing::warn!("Request receiver already taken");
                return;
            }
        };

        let handlers = match self.handlers.read().await.clone() {
            Some(h) => h,
            None => {
                tracing::error!("Handlers not initialized");
                return;
            }
        };

        tokio::spawn(async move {
            while let Some(request) = request_rx.recv().await {
                match request {
                    ClientRequest::Global {
                        request,
                        response_tx,
                    } => {
                        debug!("Processing global request: {:?}", request);
                        let result = handlers.global.handle(request).await;
                        let _ = response_tx.send(result);
                    }

                    ClientRequest::Group {
                        group_id,
                        request,
                        response_tx,
                    } => {
                        debug!(
                            "Processing group request for group {:?}: {:?}",
                            group_id, request
                        );
                        let result = handlers.group.handle(group_id, request).await;
                        let _ = response_tx.send(result);
                    }

                    ClientRequest::Stream {
                        stream_name,
                        request,
                        response_tx,
                    } => {
                        debug!("Processing stream request for: {}", stream_name);
                        let result = handlers.stream.handle(&stream_name, request).await;
                        let _ = response_tx.send(result);
                    }

                    ClientRequest::GetStreamInfo {
                        stream_name,
                        response_tx,
                    } => {
                        debug!("Processing stream info request for: {}", stream_name);
                        let result = handlers.query.get_stream_info(&stream_name).await;
                        let _ = response_tx.send(result);
                    }

                    ClientRequest::GetGroupInfo {
                        group_id,
                        response_tx,
                    } => {
                        debug!("Processing group info request for: {:?}", group_id);
                        let result = handlers.query.get_group_info(group_id).await;
                        let _ = response_tx.send(result);
                    }
                }
            }

            info!("Client request processor stopped");
        });
    }

    /// Register network handlers for forwarded requests
    async fn register_network_handlers(&self) -> ConsensusResult<()> {
        let network_guard = self.network_manager.read().await;
        let network = network_guard.as_ref().ok_or_else(|| {
            Error::with_context(
                crate::error::ErrorKind::Service,
                "Network manager not available",
            )
        })?;

        let handlers = self.handlers.clone();

        network
            .register_service::<super::messages::ClientServiceMessage, _>(move |sender, message| {
                let handlers = handlers.clone();

                Box::pin(async move {
                    let handlers = handlers.read().await.clone().ok_or_else(|| {
                        proven_network::NetworkError::Internal(
                            "Handlers not initialized".to_string(),
                        )
                    })?;

                    match message {
                        super::messages::ClientServiceMessage::Global {
                            requester_id: _,
                            request,
                        } => {
                            tracing::info!("Received forwarded global request from {}", sender);
                            let response = handlers.global.handle(request).await.map_err(|e| {
                                proven_network::NetworkError::Internal(format!(
                                    "Failed to handle global request: {e}"
                                ))
                            })?;
                            Ok(super::messages::ClientServiceResponse::Global { response })
                        }

                        super::messages::ClientServiceMessage::Group {
                            requester_id: _,
                            group_id,
                            request,
                        } => {
                            tracing::info!(
                                "Received forwarded group request from {} for group {:?}",
                                sender,
                                group_id
                            );
                            let response =
                                handlers
                                    .group
                                    .handle(group_id, request)
                                    .await
                                    .map_err(|e| {
                                        proven_network::NetworkError::Internal(format!(
                                            "Failed to handle group request: {e}"
                                        ))
                                    })?;
                            Ok(super::messages::ClientServiceResponse::Group { response })
                        }

                        super::messages::ClientServiceMessage::StreamRead {
                            requester_id: _,
                            stream_name,
                            start_sequence,
                            count,
                        } => {
                            tracing::info!(
                                "Received forwarded read request from {} for stream {}",
                                sender,
                                stream_name
                            );

                            let messages = handlers
                                .stream_read
                                .handle_read(&stream_name, start_sequence, count)
                                .await
                                .map_err(|e| {
                                    proven_network::NetworkError::Internal(format!(
                                        "Failed to read stream: {e}"
                                    ))
                                })?;

                            Ok(super::messages::ClientServiceResponse::StreamRead { messages })
                        }
                        super::messages::ClientServiceMessage::StreamStart {
                            requester_id: _,
                            stream_name,
                            start_sequence,
                            end_sequence,
                            batch_size,
                        } => {
                            tracing::info!(
                                "Received stream start request from {} for stream {}",
                                sender,
                                stream_name
                            );

                            let stream_handler =
                                handlers.stream_streaming.as_ref().ok_or_else(|| {
                                    proven_network::NetworkError::Internal(
                                        "Streaming not supported".to_string(),
                                    )
                                })?;

                            let (session_id, messages, has_more, next_sequence) = stream_handler
                                .start_stream(
                                    sender.clone(),
                                    stream_name,
                                    LogIndex::new(start_sequence).unwrap(),
                                    end_sequence.and_then(LogIndex::new),
                                    batch_size,
                                )
                                .await
                                .map_err(|e| {
                                    proven_network::NetworkError::Internal(format!(
                                        "Failed to start stream: {e}"
                                    ))
                                })?;

                            Ok(super::messages::ClientServiceResponse::StreamBatch {
                                session_id,
                                messages,
                                has_more,
                                next_sequence: next_sequence.map(|n| n.get()),
                            })
                        }
                        super::messages::ClientServiceMessage::StreamContinue {
                            requester_id: _,
                            session_id,
                            max_messages,
                        } => {
                            tracing::debug!(
                                "Received stream continue request from {} for session {}",
                                sender,
                                session_id
                            );

                            let stream_handler =
                                handlers.stream_streaming.as_ref().ok_or_else(|| {
                                    proven_network::NetworkError::Internal(
                                        "Streaming not supported".to_string(),
                                    )
                                })?;

                            let (messages, has_more, next_sequence) = stream_handler
                                .continue_stream(session_id, max_messages)
                                .await
                                .map_err(|e| {
                                    proven_network::NetworkError::Internal(format!(
                                        "Failed to continue stream: {e}"
                                    ))
                                })?;

                            Ok(super::messages::ClientServiceResponse::StreamBatch {
                                session_id,
                                messages,
                                has_more,
                                next_sequence: next_sequence.map(|n| n.get()),
                            })
                        }
                        super::messages::ClientServiceMessage::StreamCancel {
                            requester_id: _,
                            session_id,
                        } => {
                            tracing::debug!(
                                "Received stream cancel request from {} for session {}",
                                sender,
                                session_id
                            );

                            let stream_handler =
                                handlers.stream_streaming.as_ref().ok_or_else(|| {
                                    proven_network::NetworkError::Internal(
                                        "Streaming not supported".to_string(),
                                    )
                                })?;

                            stream_handler
                                .cancel_stream(session_id)
                                .await
                                .map_err(|e| {
                                    proven_network::NetworkError::Internal(format!(
                                        "Failed to cancel stream: {e}"
                                    ))
                                })?;

                            Ok(super::messages::ClientServiceResponse::StreamEnd {
                                session_id,
                                reason: super::messages::StreamEndReason::Cancelled,
                            })
                        }
                        super::messages::ClientServiceMessage::QueryStreamInfo {
                            requester_id: _,
                            stream_name,
                        } => {
                            tracing::debug!(
                                "Received stream info query from {} for stream {}",
                                sender,
                                stream_name
                            );

                            let info = handlers.query.get_stream_info(&stream_name).await.map_err(
                                |e| {
                                    proven_network::NetworkError::Internal(format!(
                                        "Failed to get stream info: {e}"
                                    ))
                                },
                            )?;

                            Ok(super::messages::ClientServiceResponse::StreamInfo { info })
                        }
                    }
                })
            })
            .await?;

        info!("Client service network handlers registered successfully");
        Ok(())
    }

    // Public API methods (delegate to handlers via channels)

    /// Submit a global request to the client service
    pub async fn submit_global_request(
        &self,
        request: crate::consensus::global::GlobalRequest,
    ) -> ConsensusResult<crate::consensus::global::GlobalResponse> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(ClientRequest::Global {
                request,
                response_tx,
            })
            .await
            .map_err(|_| {
                Error::with_context(
                    crate::error::ErrorKind::Service,
                    "Client service not accepting requests",
                )
            })?;

        response_rx.await.map_err(|_| {
            Error::with_context(crate::error::ErrorKind::Internal, "Response channel closed")
        })?
    }

    pub async fn submit_group_request(
        &self,
        group_id: ConsensusGroupId,
        request: crate::consensus::group::GroupRequest,
    ) -> ConsensusResult<crate::consensus::group::GroupResponse> {
        tracing::debug!(
            "ClientService on node {}: submit_group_request for group {:?}, request: {:?}",
            self.node_id,
            group_id,
            request
        );

        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(ClientRequest::Group {
                group_id,
                request,
                response_tx,
            })
            .await
            .map_err(|_| {
                Error::with_context(
                    crate::error::ErrorKind::Service,
                    "Client service not accepting requests",
                )
            })?;

        response_rx.await.map_err(|_| {
            Error::with_context(crate::error::ErrorKind::Internal, "Response channel closed")
        })?
    }

    pub async fn submit_stream_request(
        &self,
        stream_name: &str,
        request: crate::consensus::group::GroupRequest,
    ) -> ConsensusResult<crate::consensus::group::GroupResponse> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(ClientRequest::Stream {
                stream_name: stream_name.to_string(),
                request,
                response_tx,
            })
            .await
            .map_err(|_| {
                Error::with_context(
                    crate::error::ErrorKind::Service,
                    "Client service not accepting requests",
                )
            })?;

        response_rx.await.map_err(|_| {
            Error::with_context(crate::error::ErrorKind::Internal, "Response channel closed")
        })?
    }

    pub async fn get_stream_info(&self, stream_name: &str) -> ConsensusResult<Option<StreamInfo>> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(ClientRequest::GetStreamInfo {
                stream_name: stream_name.to_string(),
                response_tx,
            })
            .await
            .map_err(|_| {
                Error::with_context(
                    crate::error::ErrorKind::Service,
                    "Client service not accepting requests",
                )
            })?;

        response_rx.await.map_err(|_| {
            Error::with_context(crate::error::ErrorKind::Internal, "Response channel closed")
        })?
    }

    pub async fn get_group_info(
        &self,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<Option<GroupInfo>> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(ClientRequest::GetGroupInfo {
                group_id,
                response_tx,
            })
            .await
            .map_err(|_| {
                Error::with_context(
                    crate::error::ErrorKind::Service,
                    "Client service not accepting requests",
                )
            })?;

        response_rx.await.map_err(|_| {
            Error::with_context(crate::error::ErrorKind::Internal, "Response channel closed")
        })?
    }

    /// Check if a stream is local to this node
    pub async fn is_stream_local(&self, stream_name: &str) -> ConsensusResult<bool> {
        let route_info = self
            .routing_table
            .get_stream_route(stream_name)
            .await
            .map_err(|e| {
                Error::with_context(
                    crate::error::ErrorKind::Service,
                    format!("Failed to get routing info: {e}"),
                )
            })?
            .ok_or_else(|| {
                Error::with_context(
                    crate::error::ErrorKind::NotFound,
                    format!("Stream '{stream_name}' not found"),
                )
            })?;

        // Check if the group that owns this stream is local
        Ok(self
            .routing_table
            .is_group_local(route_info.group_id)
            .await
            .unwrap_or(false))
    }

    /// Start a streaming session for a stream
    pub async fn start_streaming_session(
        &self,
        stream_name: &str,
        start_sequence: LogIndex,
        end_sequence: Option<LogIndex>,
        batch_size: LogIndex,
    ) -> ConsensusResult<(
        uuid::Uuid,
        Vec<crate::services::stream::StoredMessage>,
        bool,
    )> {
        let route_info = self
            .routing_table
            .get_stream_route(stream_name)
            .await
            .map_err(|e| {
                Error::with_context(
                    crate::error::ErrorKind::Service,
                    format!("Failed to get routing info: {e}"),
                )
            })?
            .ok_or_else(|| {
                Error::with_context(
                    crate::error::ErrorKind::NotFound,
                    format!("Stream '{stream_name}' not found"),
                )
            })?;

        let is_local = self
            .routing_table
            .is_group_local(route_info.group_id)
            .await
            .unwrap_or(false);

        if is_local {
            // Stream is local
            let handlers_guard = self.handlers.read().await;
            let handlers = handlers_guard.as_ref().ok_or_else(|| {
                Error::with_context(crate::error::ErrorKind::Service, "Handlers not initialized")
            })?;

            let streaming_handler = handlers.stream_streaming.as_ref().ok_or_else(|| {
                Error::with_context(crate::error::ErrorKind::Service, "Streaming not supported")
            })?;

            let (session_id, messages, has_more, _next) = streaming_handler
                .start_stream(
                    self.node_id.clone(),
                    stream_name.to_string(),
                    start_sequence,
                    end_sequence,
                    batch_size,
                )
                .await?;

            Ok((session_id, messages, has_more))
        } else {
            // Stream is remote
            self.forwarder
                .forward_streaming_start(
                    route_info.group_id,
                    stream_name,
                    start_sequence,
                    end_sequence,
                    batch_size,
                )
                .await
        }
    }

    /// Continue a streaming session
    pub async fn continue_streaming_session(
        &self,
        session_id: uuid::Uuid,
        max_messages: LogIndex,
    ) -> ConsensusResult<(Vec<crate::services::stream::StoredMessage>, bool)> {
        // For continuing a session, we need to know if it's local or remote
        // We'll check if we have a local session first
        let handlers_guard = self.handlers.read().await;
        let handlers = handlers_guard.as_ref().ok_or_else(|| {
            Error::with_context(crate::error::ErrorKind::Service, "Handlers not initialized")
        })?;

        if let Some(streaming_handler) = handlers.stream_streaming.as_ref() {
            // Try to continue the local session
            match streaming_handler
                .continue_stream(session_id, max_messages.get() as u32)
                .await
            {
                Ok((messages, has_more, _next)) => Ok((messages, has_more)),
                Err(e) if *e.kind() == crate::error::ErrorKind::NotFound => {
                    // Session not found locally, it might be remote
                    self.forwarder
                        .forward_streaming_continue(session_id, max_messages.get() as u32)
                        .await
                }
                Err(e) => Err(e),
            }
        } else {
            // No local streaming handler, must be remote
            self.forwarder
                .forward_streaming_continue(session_id, max_messages.get() as u32)
                .await
        }
    }

    /// Cancel a streaming session
    pub async fn cancel_streaming_session(&self, session_id: uuid::Uuid) -> ConsensusResult<()> {
        // Try to cancel locally first
        let handlers_guard = self.handlers.read().await;
        let handlers = handlers_guard.as_ref().ok_or_else(|| {
            Error::with_context(crate::error::ErrorKind::Service, "Handlers not initialized")
        })?;

        if let Some(streaming_handler) = handlers.stream_streaming.as_ref() {
            match streaming_handler.cancel_stream(session_id).await {
                Ok(()) => Ok(()),
                Err(e) if *e.kind() == crate::error::ErrorKind::NotFound => {
                    // Session not found locally, try remote
                    self.forwarder.forward_streaming_cancel(session_id).await
                }
                Err(e) => Err(e),
            }
        } else {
            // No local handler, must be remote
            self.forwarder.forward_streaming_cancel(session_id).await
        }
    }

    /// Start periodic cleanup for streaming sessions
    async fn start_streaming_cleanup(&self, mut shutdown_rx: oneshot::Receiver<()>) {
        let handlers = self.handlers.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Some(handlers) = handlers.read().await.as_ref()
                            && let Some(streaming_handler) = handlers.stream_streaming.as_ref() {
                                streaming_handler.cleanup_expired_sessions().await;
                                let active_count = streaming_handler.active_session_count().await;
                                if active_count > 0 {
                                    debug!("Active streaming sessions: {}", active_count);
                                }
                            }
                    }
                    _ = &mut shutdown_rx => {
                        debug!("Streaming cleanup task shutting down");
                        break;
                    }
                }
            }
        });
    }

    // ===== PubSub Methods =====

    /// Publish a message to a PubSub subject
    pub async fn publish_message(
        &self,
        subject: &str,
        payload: bytes::Bytes,
        headers: Vec<(String, String)>,
    ) -> ConsensusResult<()> {
        use crate::foundation::types::Subject;
        use crate::services::client::events::ClientServiceEvent;
        use uuid::Uuid;

        // Validate subject
        let subject = Subject::new(subject.to_string()).map_err(|e| {
            Error::with_context(
                crate::error::ErrorKind::InvalidState,
                format!("Invalid subject: {e}"),
            )
        })?;

        // Create command request
        let request = crate::services::pubsub::PublishMessage {
            subject,
            payload,
            headers,
        };

        // Send command and wait for response
        self.event_bus.request(request).await.map_err(|e| {
            Error::with_context(
                crate::error::ErrorKind::Service,
                format!("Publish request failed: {e}"),
            )
        })
    }

    /// Subscribe to a PubSub subject pattern
    pub async fn subscribe_to_subject(
        &self,
        subject_pattern: &str,
        queue_group: Option<String>,
    ) -> ConsensusResult<(
        String,
        tokio::sync::broadcast::Receiver<crate::services::pubsub::PubSubMessage>,
    )> {
        use crate::foundation::types::SubjectPattern;
        use crate::services::client::events::ClientServiceEvent;
        use crate::services::pubsub::PubSubMessage;
        use uuid::Uuid;

        // Validate subject pattern
        let subject_pattern = SubjectPattern::new(subject_pattern.to_string()).map_err(|e| {
            Error::with_context(
                crate::error::ErrorKind::InvalidState,
                format!("Invalid subject pattern: {e}"),
            )
        })?;

        // Create broadcast channel for messages with larger capacity for benchmarks
        // TODO: Make this configurable
        let (message_tx, message_rx) = tokio::sync::broadcast::channel::<PubSubMessage>(100_000);

        // Create command request
        let request = crate::services::pubsub::Subscribe {
            subject_pattern,
            queue_group,
            message_tx,
        };

        // Send command and wait for response
        match self.event_bus.request(request).await {
            Ok(subscription_id) => Ok((subscription_id, message_rx)),
            Err(e) => Err(Error::with_context(
                crate::error::ErrorKind::Service,
                format!("Subscribe request failed: {e}"),
            )),
        }
    }

    /// Subscribe to a PubSub subject pattern using efficient streaming
    ///
    /// This method uses the new streaming API which eliminates the intermediate
    /// broadcast channel for better performance and lower memory usage.
    pub async fn subscribe_to_subject_stream(
        &self,
        subject_pattern: &str,
        queue_group: Option<String>,
    ) -> ConsensusResult<flume::Receiver<crate::services::pubsub::PubSubMessage>> {
        use crate::foundation::types::SubjectPattern;
        use crate::services::pubsub::{PubSubMessage, SubscribeStream};

        // Validate subject pattern
        let subject_pattern = SubjectPattern::new(subject_pattern.to_string()).map_err(|e| {
            Error::with_context(
                crate::error::ErrorKind::InvalidState,
                format!("Invalid subject pattern: {e}"),
            )
        })?;

        // Create streaming subscription request
        let request = SubscribeStream {
            subject_pattern,
            queue_group,
        };

        // Send streaming request and get receiver
        self.event_bus.stream(request).await.map_err(|e| {
            Error::with_context(
                crate::error::ErrorKind::Service,
                format!("Streaming subscribe request failed: {e}"),
            )
        })
    }

    /// Subscribe to a PubSub subject pattern and get an async stream
    ///
    /// This is a convenience method that converts the flume receiver into
    /// an async stream for easier integration with async code.
    pub async fn subscribe_to_subject_iter(
        &self,
        subject_pattern: &str,
    ) -> ConsensusResult<impl futures::Stream<Item = crate::services::pubsub::PubSubMessage>> {
        let receiver = self
            .subscribe_to_subject_stream(subject_pattern, None)
            .await?;
        Ok(receiver.into_stream())
    }

    /// Unsubscribe from a PubSub subscription
    pub async fn unsubscribe_from_subject(&self, subscription_id: &str) -> ConsensusResult<()> {
        // Create command request
        let request = crate::services::pubsub::Unsubscribe {
            subscription_id: subscription_id.to_string(),
        };

        // Send command and wait for response
        self.event_bus.request(request).await.map_err(|e| {
            Error::with_context(
                crate::error::ErrorKind::Service,
                format!("Unsubscribe request failed: {e}"),
            )
        })
    }

    /// Stream messages directly from a stream without session management
    ///
    /// This uses the new event bus streaming feature for efficient message delivery
    pub async fn stream_messages(
        &self,
        stream_name: &str,
        start_sequence: LogIndex,
        end_sequence: Option<LogIndex>,
    ) -> ConsensusResult<flume::r#async::RecvStream<'static, crate::services::stream::StoredMessage>>
    {
        use crate::services::stream::StreamMessages;

        let request = StreamMessages {
            stream_name: stream_name.to_string(),
            start_sequence,
            end_sequence,
        };

        // Use event bus streaming
        let receiver = self.event_bus.stream(request).await.map_err(|e| {
            Error::with_context(
                crate::error::ErrorKind::Service,
                format!("Failed to start streaming: {e}"),
            )
        })?;

        // Convert the receiver to a Stream
        Ok(receiver.into_stream())
    }
}

#[async_trait::async_trait]
impl<T, G, S> ServiceLifecycle for ClientService<T, G, S>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    S: StorageAdaptor + 'static,
{
    async fn initialize(&self) -> ConsensusResult<()> {
        Ok(())
    }

    async fn start(&self) -> ConsensusResult<()> {
        info!("Starting ClientService");

        // Check that required services are set
        if self.global_consensus.read().await.is_none() {
            return Err(Error::with_context(
                crate::error::ErrorKind::Configuration,
                "Global consensus service not set",
            ));
        }
        if self.group_consensus.read().await.is_none() {
            return Err(Error::with_context(
                crate::error::ErrorKind::Configuration,
                "Group consensus service not set",
            ));
        }

        // Initialize handlers
        self.initialize_handlers().await?;

        // Register network handlers if network manager is available
        if self.network_manager.read().await.is_some()
            && let Err(e) = self.register_network_handlers().await
        {
            tracing::warn!("Failed to register network handlers: {}", e);
            // Continue anyway - the service can still work for local requests
        }

        // Mark as running
        *self.is_running.write().await = true;

        // Set up shutdown channel
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        *self.shutdown.write().await = Some(shutdown_tx);

        // Start processing requests
        self.process_requests().await;

        // Start periodic cleanup for streaming sessions
        self.start_streaming_cleanup(shutdown_rx).await;

        info!("ClientService started successfully");
        Ok(())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        info!("Stopping ClientService");

        // Signal shutdown
        if let Some(shutdown_tx) = self.shutdown.write().await.take() {
            let _ = shutdown_tx.send(());
        }

        // Unregister the service handler from NetworkManager
        use super::messages::ClientServiceMessage;
        if let Some(network) = self.network_manager.read().await.as_ref() {
            tracing::debug!("Unregistering client service handler");
            if let Err(e) = network.unregister_service::<ClientServiceMessage>().await {
                tracing::warn!("Failed to unregister client service: {}", e);
            }
        }

        // Drop the request receiver to stop processing
        self.request_rx.write().await.take();

        // Mark as not running
        *self.is_running.write().await = false;

        info!("ClientService stopped");
        Ok(())
    }

    async fn is_healthy(&self) -> bool {
        *self.is_running.read().await
    }

    async fn status(&self) -> crate::foundation::traits::lifecycle::ServiceStatus {
        use crate::foundation::traits::lifecycle::ServiceStatus;
        if *self.is_running.read().await {
            ServiceStatus::Running
        } else {
            ServiceStatus::Stopped
        }
    }
}
