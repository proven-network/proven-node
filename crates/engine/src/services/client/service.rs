//! Simplified client service implementation
//!
//! The ClientService acts as a thin coordinator, delegating requests to specialized handlers.

use std::{num::NonZero, sync::Arc};

use proven_network::NetworkManager;
use proven_storage::StorageAdaptor;
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;
use tokio::sync::{RwLock, mpsc, oneshot};
use tracing::{debug, info};

use crate::{
    error::{ConsensusResult, Error},
    foundation::{traits::ServiceLifecycle, types::ConsensusGroupId},
    services::{
        event::{EventBus, EventService},
        global_consensus::GlobalConsensusService,
        group_consensus::GroupConsensusService,
        routing::RoutingService,
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
    routing_service: ServiceRef<RoutingService>,
    stream_service: StreamServiceRef<S>,
    network_manager: NetworkManagerRef<T, G>,

    /// Event bus for publishing events
    event_bus: Arc<EventBus>,

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
    pub fn new(node_id: NodeId, event_bus: Arc<EventBus>) -> Self {
        let (request_tx, request_rx) = mpsc::channel(1000);

        // Service references
        let global_consensus = Arc::new(RwLock::new(None));
        let group_consensus = Arc::new(RwLock::new(None));
        let routing_service = Arc::new(RwLock::new(None));
        let stream_service = Arc::new(RwLock::new(None));
        let network_manager = Arc::new(RwLock::new(None));

        // Create forwarder
        let forwarder = Arc::new(RequestForwarder::new(
            node_id.clone(),
            network_manager.clone(),
            routing_service.clone(),
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
            routing_service,
            stream_service,
            network_manager,
            event_bus,
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

    /// Set the routing service reference
    pub async fn set_routing_service(&self, service: Arc<RoutingService>) {
        *self.routing_service.write().await = Some(service);
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
        let routing_guard = self.routing_service.read().await;
        let routing = routing_guard.as_ref().ok_or_else(|| {
            Error::with_context(
                crate::error::ErrorKind::Service,
                "Routing service not available",
            )
        })?;

        let routing_info = routing.get_routing_info().await.map_err(|e| {
            Error::with_context(
                crate::error::ErrorKind::Service,
                format!("Failed to get routing info: {e}"),
            )
        })?;

        // Find local groups
        let mut local_groups: Vec<_> = routing_info
            .group_routes
            .iter()
            .filter(|(_, group)| {
                group.location == crate::services::routing::GroupLocation::Local
                    || group.location == crate::services::routing::GroupLocation::Distributed
            })
            .collect();

        if local_groups.is_empty() {
            // Try remote groups
            let mut remote_groups: Vec<_> = routing_info
                .group_routes
                .iter()
                .filter(|(_, group)| {
                    group.location == crate::services::routing::GroupLocation::Remote
                })
                .collect();

            if remote_groups.is_empty() {
                return Err(Error::with_context(
                    crate::error::ErrorKind::InvalidState,
                    "No consensus groups available",
                ));
            }

            // Sort by preference: default group first, then by stream count
            remote_groups.sort_by(|(id_a, group_a), (id_b, group_b)| {
                let default_id = ConsensusGroupId::new(1);
                if **id_a == default_id && **id_b != default_id {
                    std::cmp::Ordering::Less
                } else if **id_a != default_id && **id_b == default_id {
                    std::cmp::Ordering::Greater
                } else {
                    group_a.stream_count.cmp(&group_b.stream_count)
                }
            });

            return Ok(*remote_groups[0].0);
        }

        // Sort local groups by preference
        local_groups.sort_by(|(id_a, group_a), (id_b, group_b)| {
            let default_id = ConsensusGroupId::new(1);
            if **id_a == default_id && **id_b != default_id {
                std::cmp::Ordering::Less
            } else if **id_b == default_id && **id_a != default_id {
                std::cmp::Ordering::Greater
            } else {
                group_a.stream_count.cmp(&group_b.stream_count)
            }
        });

        Ok(*local_groups[0].0)
    }

    /// Read messages directly from a stream
    pub async fn read_stream(
        &self,
        stream_name: &str,
        start_sequence: NonZero<u64>,
        count: NonZero<u64>,
    ) -> ConsensusResult<Vec<crate::services::stream::StoredMessage>> {
        let routing_guard = self.routing_service.read().await;
        let routing = routing_guard.as_ref().ok_or_else(|| {
            Error::with_context(
                crate::error::ErrorKind::Service,
                "Routing service not available",
            )
        })?;

        let route_info = routing
            .get_stream_routing_info(stream_name)
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

        let routing_info = routing.get_routing_info().await.map_err(|e| {
            Error::with_context(
                crate::error::ErrorKind::Service,
                format!("Failed to get routing info: {e}"),
            )
        })?;

        if let Some(group_info) = routing_info.group_routes.get(&route_info.group_id) {
            if group_info.members.contains(&self.node_id) {
                // Stream is local
                let stream_guard = self.stream_service.read().await;
                let stream_service = stream_guard.as_ref().ok_or_else(|| {
                    Error::with_context(
                        crate::error::ErrorKind::Service,
                        "Stream service not available",
                    )
                })?;

                stream_service
                    .read_messages(stream_name, start_sequence, count)
                    .await
            } else {
                // Stream is remote
                self.forwarder
                    .forward_read_request(route_info.group_id, stream_name, start_sequence, count)
                    .await
            }
        } else {
            Err(Error::with_context(
                crate::error::ErrorKind::NotFound,
                format!("Group {:?} not found", route_info.group_id),
            ))
        }
    }

    /// Initialize handlers after all services are set
    async fn initialize_handlers(&self) -> ConsensusResult<()> {
        // Create handlers
        let global_handler = GlobalHandler::new(
            self.node_id.clone(),
            self.global_consensus.clone(),
            self.network_manager.clone(),
            self.routing_service.clone(),
            self.event_bus.clone(),
        );

        let group_handler = GroupHandler::new(
            self.group_consensus.clone(),
            self.routing_service.clone(),
            self.forwarder.clone(),
        );

        let stream_handler = StreamHandler::new(
            self.routing_service.clone(),
            Arc::new(group_handler.clone()),
        );

        let stream_read_handler = StreamReadHandler::new(self.stream_service.clone());

        // Create streaming handler - StorageAdaptor now requires LogStorageStreaming
        let stream_streaming = Some(StreamStreamingHandler::new(self.stream_service.clone()));

        let query_handler = QueryHandler::new(
            self.group_consensus.clone(),
            self.global_consensus.clone(),
            self.routing_service.clone(),
            self.forwarder.clone(),
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
                                    NonZero::new(start_sequence).unwrap(),
                                    end_sequence.and_then(NonZero::new),
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
        let routing_guard = self.routing_service.read().await;
        let routing = routing_guard.as_ref().ok_or_else(|| {
            Error::with_context(
                crate::error::ErrorKind::Service,
                "Routing service not available",
            )
        })?;

        let route_info = routing
            .get_stream_routing_info(stream_name)
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

        let routing_info = routing.get_routing_info().await.map_err(|e| {
            Error::with_context(
                crate::error::ErrorKind::Service,
                format!("Failed to get routing info: {e}"),
            )
        })?;

        if let Some(group_info) = routing_info.group_routes.get(&route_info.group_id) {
            Ok(group_info.members.contains(&self.node_id))
        } else {
            Ok(false)
        }
    }

    /// Start a streaming session for a stream
    pub async fn start_streaming_session(
        &self,
        stream_name: &str,
        start_sequence: NonZero<u64>,
        end_sequence: Option<NonZero<u64>>,
        batch_size: NonZero<u64>,
    ) -> ConsensusResult<(
        uuid::Uuid,
        Vec<crate::services::stream::StoredMessage>,
        bool,
    )> {
        let routing_guard = self.routing_service.read().await;
        let routing = routing_guard.as_ref().ok_or_else(|| {
            Error::with_context(
                crate::error::ErrorKind::Service,
                "Routing service not available",
            )
        })?;

        let route_info = routing
            .get_stream_routing_info(stream_name)
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

        let routing_info = routing.get_routing_info().await.map_err(|e| {
            Error::with_context(
                crate::error::ErrorKind::Service,
                format!("Failed to get routing info: {e}"),
            )
        })?;

        if let Some(group_info) = routing_info.group_routes.get(&route_info.group_id) {
            if group_info.members.contains(&self.node_id) {
                // Stream is local
                let handlers_guard = self.handlers.read().await;
                let handlers = handlers_guard.as_ref().ok_or_else(|| {
                    Error::with_context(
                        crate::error::ErrorKind::Service,
                        "Handlers not initialized",
                    )
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
        } else {
            Err(Error::with_context(
                crate::error::ErrorKind::NotFound,
                format!("Group {:?} not found", route_info.group_id),
            ))
        }
    }

    /// Continue a streaming session
    pub async fn continue_streaming_session(
        &self,
        session_id: uuid::Uuid,
        max_messages: NonZero<u64>,
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
        if self.routing_service.read().await.is_none() {
            return Err(Error::with_context(
                crate::error::ErrorKind::Configuration,
                "Routing service not set",
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
