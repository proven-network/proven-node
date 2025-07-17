//! Client service implementation
//!
//! The ClientService acts as a facade for client operations, coordinating
//! with the global and group consensus services through direct service references
//! rather than the event bus.

use std::sync::Arc;

use crate::stream::StreamStorageReader;
use proven_topology::NodeId;
use tokio::sync::{RwLock, mpsc, oneshot};
use tracing::{debug, info};

use crate::{
    consensus::{
        global::{GlobalRequest, GlobalResponse},
        group::{GroupRequest, GroupResponse, types::AdminOperation},
    },
    error::{ConsensusError, ConsensusResult},
    foundation::{
        traits::{ServiceHealth, ServiceLifecycle},
        types::ConsensusGroupId,
    },
    services::{
        event::Event, global_consensus::GlobalConsensusService,
        group_consensus::GroupConsensusService, routing::RoutingService,
    },
};

use super::types::*;

/// Type alias for optional service references
type OptionalServiceRef<T> = Arc<RwLock<Option<Arc<T>>>>;

/// Client service for handling client requests
pub struct ClientService<T, G, L>
where
    T: proven_transport::Transport,
    G: proven_topology::TopologyAdaptor,
    L: proven_storage::LogStorage,
{
    /// Service name
    name: String,

    /// Node ID
    node_id: NodeId,

    /// Request receiver
    request_rx: Arc<RwLock<Option<mpsc::Receiver<ClientRequest>>>>,

    /// Request sender (for submitting requests)
    request_tx: mpsc::Sender<ClientRequest>,

    /// Global consensus service (set after construction)
    global_consensus: OptionalServiceRef<GlobalConsensusService<T, G, L>>,

    /// Group consensus service (set after construction)
    group_consensus: OptionalServiceRef<GroupConsensusService<T, G, L>>,

    /// Routing service (set after construction)
    routing_service: OptionalServiceRef<RoutingService>,

    /// Stream service (set after construction, for direct reads)
    stream_service: OptionalServiceRef<crate::stream::StreamService<L>>,

    /// Event service (set after construction)
    event_service: Arc<RwLock<Option<Arc<crate::services::event::EventService>>>>,

    /// Whether the service is running
    is_running: Arc<RwLock<bool>>,

    /// Service shutdown signal
    shutdown: Arc<RwLock<Option<oneshot::Sender<()>>>>,

    /// Type markers
    _phantom: std::marker::PhantomData<(T, G, L)>,
}

impl<T, G, L> ClientService<T, G, L>
where
    T: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    L: proven_storage::LogStorage + 'static,
{
    /// Create a new client service
    pub fn new(node_id: NodeId) -> Self {
        let (request_tx, request_rx) = mpsc::channel(1000);

        Self {
            name: "ClientService".to_string(),
            node_id,
            request_rx: Arc::new(RwLock::new(Some(request_rx))),
            request_tx,
            global_consensus: Arc::new(RwLock::new(None)),
            group_consensus: Arc::new(RwLock::new(None)),
            routing_service: Arc::new(RwLock::new(None)),
            stream_service: Arc::new(RwLock::new(None)),
            event_service: Arc::new(RwLock::new(None)),
            is_running: Arc::new(RwLock::new(false)),
            shutdown: Arc::new(RwLock::new(None)),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Set the global consensus service reference
    pub async fn set_global_consensus(&self, service: Arc<GlobalConsensusService<T, G, L>>) {
        *self.global_consensus.write().await = Some(service);
    }

    /// Set the group consensus service reference
    pub async fn set_group_consensus(&self, service: Arc<GroupConsensusService<T, G, L>>) {
        *self.group_consensus.write().await = Some(service);
    }

    /// Set the routing service reference
    pub async fn set_routing_service(&self, service: Arc<RoutingService>) {
        *self.routing_service.write().await = Some(service);
    }

    /// Set the stream service reference
    pub async fn set_stream_service(&self, service: Arc<crate::stream::StreamService<L>>) {
        *self.stream_service.write().await = Some(service);
    }

    /// Set the event service reference
    pub async fn set_event_service(&self, service: Arc<crate::services::event::EventService>) {
        *self.event_service.write().await = Some(service);
    }

    /// Get a sender for submitting client requests
    pub fn get_request_sender(&self) -> mpsc::Sender<ClientRequest> {
        self.request_tx.clone()
    }

    /// Submit a global consensus request
    pub async fn submit_global_request(
        &self,
        request: GlobalRequest,
    ) -> ConsensusResult<GlobalResponse> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(ClientRequest::Global {
                request,
                response_tx,
            })
            .await
            .map_err(|_| {
                ConsensusError::with_context(
                    crate::error::ErrorKind::Service,
                    "Client service not accepting requests",
                )
            })?;

        response_rx.await.map_err(|_| {
            ConsensusError::with_context(
                crate::error::ErrorKind::Internal,
                "Response channel closed",
            )
        })?
    }

    /// Submit a group consensus request
    pub async fn submit_group_request(
        &self,
        group_id: ConsensusGroupId,
        request: GroupRequest,
    ) -> ConsensusResult<GroupResponse> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(ClientRequest::Group {
                group_id,
                request,
                response_tx,
            })
            .await
            .map_err(|_| {
                ConsensusError::with_context(
                    crate::error::ErrorKind::Service,
                    "Client service not accepting requests",
                )
            })?;

        response_rx.await.map_err(|_| {
            ConsensusError::with_context(
                crate::error::ErrorKind::Internal,
                "Response channel closed",
            )
        })?
    }

    /// Get stream information
    pub async fn get_stream_info(&self, stream_name: &str) -> ConsensusResult<Option<StreamInfo>> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(ClientRequest::GetStreamInfo {
                stream_name: stream_name.to_string(),
                response_tx,
            })
            .await
            .map_err(|_| {
                ConsensusError::with_context(
                    crate::error::ErrorKind::Service,
                    "Client service not accepting requests",
                )
            })?;

        response_rx.await.map_err(|_| {
            ConsensusError::with_context(
                crate::error::ErrorKind::Internal,
                "Response channel closed",
            )
        })?
    }

    /// Get group information
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
                ConsensusError::with_context(
                    crate::error::ErrorKind::Service,
                    "Client service not accepting requests",
                )
            })?;

        response_rx.await.map_err(|_| {
            ConsensusError::with_context(
                crate::error::ErrorKind::Internal,
                "Response channel closed",
            )
        })?
    }

    /// Submit a stream operation (routing will determine target group)
    pub async fn submit_stream_request(
        &self,
        stream_name: &str,
        request: GroupRequest,
    ) -> ConsensusResult<GroupResponse> {
        let (response_tx, response_rx) = oneshot::channel();

        self.request_tx
            .send(ClientRequest::Stream {
                stream_name: stream_name.to_string(),
                request,
                response_tx,
            })
            .await
            .map_err(|_| {
                ConsensusError::with_context(
                    crate::error::ErrorKind::Service,
                    "Client service not accepting requests",
                )
            })?;

        response_rx.await.map_err(|_| {
            ConsensusError::with_context(
                crate::error::ErrorKind::Internal,
                "Response channel closed",
            )
        })?
    }

    /// Read messages directly from a stream (bypasses consensus)
    pub async fn read_stream(
        &self,
        stream_name: &str,
        start_sequence: u64,
        count: u64,
    ) -> ConsensusResult<Vec<crate::stream::StoredMessage>> {
        // Get routing service to check where stream is located
        let routing_service = self.routing_service.read().await;
        let routing_service = routing_service.as_ref().ok_or_else(|| {
            ConsensusError::with_context(
                crate::error::ErrorKind::Service,
                "Routing service not available",
            )
        })?;

        // Get routing info to determine if stream is local
        let route_info = routing_service
            .get_stream_routing_info(stream_name)
            .await
            .map_err(|e| {
                ConsensusError::with_context(
                    crate::error::ErrorKind::Service,
                    format!("Failed to get routing info: {e}"),
                )
            })?
            .ok_or_else(|| {
                ConsensusError::with_context(
                    crate::error::ErrorKind::NotFound,
                    format!("Stream '{stream_name}' not found"),
                )
            })?;

        // Check if stream is on this node
        let routing_info = routing_service.get_routing_info().await.map_err(|e| {
            ConsensusError::with_context(
                crate::error::ErrorKind::Service,
                format!("Failed to get routing info: {e}"),
            )
        })?;

        if let Some(group_info) = routing_info.group_routes.get(&route_info.group_id) {
            if group_info.members.contains(&self.node_id) {
                // Stream is local - directly call StreamService
                let stream_service = self.stream_service.read().await;
                let stream_service = stream_service.as_ref().ok_or_else(|| {
                    ConsensusError::with_context(
                        crate::error::ErrorKind::Service,
                        "Stream service not available",
                    )
                })?;

                stream_service
                    .read_messages(stream_name, start_sequence, count)
                    .await
            } else {
                // Stream is remote - would need to forward
                Err(ConsensusError::with_context(
                    crate::error::ErrorKind::Service,
                    format!(
                        "Stream '{}' is not available on this node (group {:?})",
                        stream_name, route_info.group_id
                    ),
                ))
            }
        } else {
            Err(ConsensusError::with_context(
                crate::error::ErrorKind::NotFound,
                format!("Group {:?} not found", route_info.group_id),
            ))
        }
    }

    /// Process incoming client requests
    async fn process_requests(&self) {
        let mut request_rx = self
            .request_rx
            .write()
            .await
            .take()
            .expect("Request receiver already taken");
        let global_consensus = self.global_consensus.clone();
        let group_consensus = self.group_consensus.clone();
        let routing_service = self.routing_service.clone();
        let event_service = self.event_service.clone();
        let node_id = self.node_id.clone();

        tokio::spawn(async move {
            while let Some(request) = request_rx.recv().await {
                match request {
                    ClientRequest::Global {
                        request,
                        response_tx,
                    } => {
                        debug!("Processing global request: {:?}", request);

                        // Check if this is a request we can handle directly
                        match &request {
                            GlobalRequest::CreateGroup { info } => {
                                // For group creation, bypass global consensus and create directly
                                let group = group_consensus.read().await;
                                if let Some(service) = group.as_ref() {
                                    // Create the group
                                    match service.create_group(info.id, info.members.clone()).await
                                    {
                                        Ok(_) => {
                                            // Update routing service with group info
                                            let routing = routing_service.read().await;
                                            if let Some(routing_service) = routing.as_ref() {
                                                let group_route = crate::services::routing::GroupRoute {
                                                    group_id: info.id,
                                                    members: info.members.clone(),
                                                    leader: info.members.first().cloned(),
                                                    stream_count: 0,
                                                    health: crate::services::routing::GroupHealth::Healthy,
                                                    last_updated: std::time::SystemTime::now(),
                                                };
                                                if let Err(e) = routing_service
                                                    .update_group_info(info.id, group_route)
                                                    .await
                                                {
                                                    tracing::warn!(
                                                        "Failed to update routing for group {}: {}",
                                                        info.id,
                                                        e
                                                    );
                                                }
                                            }

                                            let global_response =
                                                GlobalResponse::GroupCreated { id: info.id };
                                            let _ = response_tx.send(Ok(global_response));
                                        }
                                        Err(e) => {
                                            let _ = response_tx.send(Err(e));
                                        }
                                    }
                                } else {
                                    let _ = response_tx.send(Err(ConsensusError::with_context(
                                        crate::error::ErrorKind::Service,
                                        "Group consensus service not available",
                                    )));
                                }
                            }
                            GlobalRequest::CreateStream {
                                name,
                                group_id,
                                config,
                            } => {
                                // For stream creation, bypass global consensus and create directly in specified group
                                let target_group_id = *group_id;

                                // Submit to group consensus to initialize the stream
                                let group_request =
                                    GroupRequest::Admin(AdminOperation::InitializeStream {
                                        stream: name.clone(),
                                    });

                                let group = group_consensus.read().await;
                                if let Some(service) = group.as_ref() {
                                    match service
                                        .submit_to_group(target_group_id, group_request)
                                        .await
                                    {
                                        Ok(group_response) => {
                                            // Convert group response to global response
                                            match group_response {
                                                GroupResponse::Success => {
                                                    // Emit StreamCreated event
                                                    let event_service_guard =
                                                        event_service.read().await;
                                                    if let Some(event_svc) =
                                                        event_service_guard.as_ref()
                                                    {
                                                        let event = Event::StreamCreated {
                                                            name: name.clone(),
                                                            config: config.clone(),
                                                            group_id: target_group_id,
                                                        };
                                                        let source =
                                                            format!("client-service-{node_id}");
                                                        let event_svc = event_svc.clone();
                                                        tokio::spawn(async move {
                                                            if let Err(e) = event_svc
                                                                .publish(event, source)
                                                                .await
                                                            {
                                                                tracing::warn!(
                                                                    "Failed to publish StreamCreated event: {}",
                                                                    e
                                                                );
                                                            }
                                                        });
                                                    }

                                                    let global_response =
                                                        GlobalResponse::StreamCreated {
                                                            name: name.clone(),
                                                            group_id: target_group_id,
                                                        };
                                                    let _ = response_tx.send(Ok(global_response));
                                                }
                                                GroupResponse::Error { message } => {
                                                    let _ = response_tx.send(Err(
                                                        ConsensusError::with_context(
                                                            crate::error::ErrorKind::Internal,
                                                            format!(
                                                                "Failed to create stream: {message}"
                                                            ),
                                                        ),
                                                    ));
                                                }
                                                _ => {
                                                    let _ = response_tx.send(Err(ConsensusError::with_context(
                                                        crate::error::ErrorKind::Internal,
                                                        "Unexpected response from group consensus",
                                                    )));
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            let _ = response_tx.send(Err(e));
                                        }
                                    }
                                } else {
                                    let _ = response_tx.send(Err(ConsensusError::with_context(
                                        crate::error::ErrorKind::Service,
                                        "Group consensus service not available",
                                    )));
                                }
                            }
                            _ => {
                                // Other global requests still not implemented
                                let global = global_consensus.read().await;
                                if let Some(_service) = global.as_ref() {
                                    let _ = response_tx.send(Err(ConsensusError::with_context(
                                        crate::error::ErrorKind::Internal,
                                        "Global consensus operation submission not yet implemented",
                                    )));
                                } else {
                                    let _ = response_tx.send(Err(ConsensusError::with_context(
                                        crate::error::ErrorKind::Service,
                                        "Global consensus service not available",
                                    )));
                                }
                            }
                        }
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

                        // Get group consensus service
                        let group = group_consensus.read().await;
                        if let Some(service) = group.as_ref() {
                            // Submit to specific group
                            match service.submit_to_group(group_id, request).await {
                                Ok(response) => {
                                    let _ = response_tx.send(Ok(response));
                                }
                                Err(e) => {
                                    let _ = response_tx.send(Err(e));
                                }
                            }
                        } else {
                            let _ = response_tx.send(Err(ConsensusError::with_context(
                                crate::error::ErrorKind::Service,
                                "Group consensus service not available",
                            )));
                        }
                    }

                    ClientRequest::GetStreamInfo {
                        stream_name,
                        response_tx,
                    } => {
                        debug!("Processing stream info request for: {}", stream_name);

                        // Get routing service for stream info
                        let routing = routing_service.read().await;
                        if let Some(service) = routing.as_ref() {
                            // Check if stream exists in routing table
                            match service.get_stream_routing_info(&stream_name).await {
                                Ok(Some(route)) => {
                                    // Query the group consensus for stream state
                                    let group = group_consensus.read().await;
                                    if let Some(group_service) = group.as_ref() {
                                        // Get the stream state from the group
                                        match group_service
                                            .get_stream_state(route.group_id, &stream_name)
                                            .await
                                        {
                                            Ok(Some(stream_state)) => {
                                                // Stream exists with state info
                                                let stream_info = StreamInfo {
                                                    name: stream_name.clone(),
                                                    config: route.config.unwrap_or({
                                                        // Fallback config if not stored in route
                                                        crate::stream::StreamConfig {
                                                            max_message_size: 1024 * 1024,
                                                            retention:
                                                                crate::stream::config::RetentionPolicy::Forever,
                                                            persistence_type:
                                                                crate::stream::PersistenceType::Persistent,
                                                            allow_auto_create: false,
                                                        }
                                                    }),
                                                    group_id: route.group_id,
                                                    last_sequence: stream_state.next_sequence.saturating_sub(1),
                                                };
                                                let _ = response_tx.send(Ok(Some(stream_info)));
                                            }
                                            Ok(None) => {
                                                // Stream exists in routing but not in group state (inconsistency)
                                                let _ = response_tx.send(Ok(None));
                                            }
                                            Err(e) => {
                                                let _ = response_tx.send(Err(e));
                                            }
                                        }
                                    } else {
                                        let _ =
                                            response_tx.send(Err(ConsensusError::with_context(
                                                crate::error::ErrorKind::Service,
                                                "Group consensus service not available",
                                            )));
                                    }
                                }
                                Ok(None) => {
                                    // Stream not found
                                    let _ = response_tx.send(Ok(None));
                                }
                                Err(e) => {
                                    let _ = response_tx.send(Err(ConsensusError::with_context(
                                        crate::error::ErrorKind::Service,
                                        format!("Failed to query routing info: {e}"),
                                    )));
                                }
                            }
                        } else {
                            let _ = response_tx.send(Err(ConsensusError::with_context(
                                crate::error::ErrorKind::Service,
                                "Routing service not available",
                            )));
                        }
                    }

                    ClientRequest::GetGroupInfo {
                        group_id,
                        response_tx,
                    } => {
                        debug!("Processing group info request for: {:?}", group_id);

                        // Get group info from global consensus
                        let global = global_consensus.read().await;
                        if let Some(_service) = global.as_ref() {
                            // TODO: Get group info from global consensus
                            // match service.get_group_info(group_id).await {
                            //     Ok(info) => {
                            //         let _ = response_tx.send(Ok(info));
                            //     }
                            //     Err(e) => {
                            //         let _ = response_tx.send(Err(e));
                            //     }
                            // }
                            let _ = response_tx.send(Err(ConsensusError::with_context(
                                crate::error::ErrorKind::Internal,
                                "Group info query not yet implemented",
                            )));
                        } else {
                            let _ = response_tx.send(Err(ConsensusError::with_context(
                                crate::error::ErrorKind::Service,
                                "Global consensus service not available",
                            )));
                        }
                    }

                    ClientRequest::Stream {
                        stream_name,
                        request,
                        response_tx,
                    } => {
                        debug!("Processing stream request for: {}", stream_name);

                        // Get routing service to determine target group
                        let routing = routing_service.read().await;
                        if let Some(routing_service) = routing.as_ref() {
                            // Route the stream operation
                            match routing_service
                                .route_stream_operation(&stream_name, vec![])
                                .await
                            {
                                Ok(crate::services::routing::RouteDecision::RouteToGroup(
                                    group_id,
                                )) => {
                                    debug!(
                                        "Routing stream {} operation to group {:?}",
                                        stream_name, group_id
                                    );

                                    // Check if we're part of this group
                                    if let Ok(Some(group_info)) = routing_service
                                        .get_routing_info()
                                        .await
                                        .map(|info| info.group_routes.get(&group_id).cloned())
                                    {
                                        if group_info.members.contains(&node_id) {
                                            // We're part of the group, submit locally
                                            let group = group_consensus.read().await;
                                            if let Some(service) = group.as_ref() {
                                                // Submit to group consensus
                                                match service
                                                    .submit_to_group(group_id, request)
                                                    .await
                                                {
                                                    Ok(response) => {
                                                        let _ = response_tx.send(Ok(response));
                                                    }
                                                    Err(e) => {
                                                        let _ = response_tx.send(Err(e));
                                                    }
                                                }
                                            } else {
                                                let _ = response_tx.send(Err(
                                                    ConsensusError::with_context(
                                                        crate::error::ErrorKind::Service,
                                                        "Group consensus service not available",
                                                    ),
                                                ));
                                            }
                                        } else {
                                            // Not part of the group, need to forward
                                            // Select a target node (prefer leader if available)
                                            let target_node = group_info
                                                .leader
                                                .clone()
                                                .or_else(|| group_info.members.first().cloned());

                                            if let Some(target) = target_node {
                                                // TODO: Use transport layer to forward request
                                                let _ = response_tx.send(Err(ConsensusError::with_context(
                                                    crate::error::ErrorKind::Service,
                                                    format!(
                                                        "Node not part of group {group_id:?}, would forward to node {target:?} (not yet implemented)"
                                                    ),
                                                )));
                                            } else {
                                                let _ = response_tx.send(Err(
                                                    ConsensusError::with_context(
                                                        crate::error::ErrorKind::InvalidState,
                                                        format!(
                                                            "Group {group_id:?} has no members"
                                                        ),
                                                    ),
                                                ));
                                            }
                                        }
                                    } else {
                                        let _ =
                                            response_tx.send(Err(ConsensusError::with_context(
                                                crate::error::ErrorKind::NotFound,
                                                format!(
                                                    "Group {group_id:?} not found in routing table"
                                                ),
                                            )));
                                    }
                                }
                                Ok(crate::services::routing::RouteDecision::Reject(reason)) => {
                                    let _ = response_tx.send(Err(ConsensusError::with_context(
                                        crate::error::ErrorKind::Validation,
                                        format!("Operation rejected: {reason}"),
                                    )));
                                }
                                Ok(decision) => {
                                    let _ = response_tx.send(Err(ConsensusError::with_context(
                                        crate::error::ErrorKind::Internal,
                                        format!("Unexpected routing decision: {decision:?}"),
                                    )));
                                }
                                Err(e) => {
                                    let _ = response_tx.send(Err(ConsensusError::with_context(
                                        crate::error::ErrorKind::Service,
                                        format!("Routing failed: {e}"),
                                    )));
                                }
                            }
                        } else {
                            let _ = response_tx.send(Err(ConsensusError::with_context(
                                crate::error::ErrorKind::Service,
                                "Routing service not available",
                            )));
                        }
                    }
                }
            }

            info!("Client request processor stopped");
        });
    }

    /// Forward a request to a node in the target group
    async fn forward_to_group(
        &self,
        group_id: ConsensusGroupId,
        members: &[NodeId],
        _request: GroupRequest,
    ) -> ConsensusResult<GroupResponse> {
        // TODO: Implement actual forwarding logic
        // This would typically:
        // 1. Select a node from the group (preferably the leader)
        // 2. Use the transport layer to send the request
        // 3. Wait for and return the response

        // For now, return an error indicating forwarding is not implemented
        Err(ConsensusError::with_context(
            crate::error::ErrorKind::Internal,
            format!(
                "Request forwarding to group {:?} not yet implemented. Target nodes: {:?}",
                group_id,
                members.iter().take(3).collect::<Vec<_>>()
            ),
        ))
    }
}

#[async_trait::async_trait]
impl<T, G, L> ServiceLifecycle for ClientService<T, G, L>
where
    T: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    L: proven_storage::LogStorage + 'static,
{
    async fn start(&self) -> ConsensusResult<()> {
        info!("Starting ClientService");

        // Check that required services are set
        if self.global_consensus.read().await.is_none() {
            return Err(ConsensusError::with_context(
                crate::error::ErrorKind::Configuration,
                "Global consensus service not set",
            ));
        }
        if self.group_consensus.read().await.is_none() {
            return Err(ConsensusError::with_context(
                crate::error::ErrorKind::Configuration,
                "Group consensus service not set",
            ));
        }
        if self.routing_service.read().await.is_none() {
            return Err(ConsensusError::with_context(
                crate::error::ErrorKind::Configuration,
                "Routing service not set",
            ));
        }

        // Mark as running
        *self.is_running.write().await = true;

        // Set up shutdown channel
        let (shutdown_tx, _) = oneshot::channel();
        *self.shutdown.write().await = Some(shutdown_tx);

        // Start processing requests
        self.process_requests().await;

        info!("ClientService started successfully");
        Ok(())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        info!("Stopping ClientService");

        // Signal shutdown
        if let Some(shutdown_tx) = self.shutdown.write().await.take() {
            let _ = shutdown_tx.send(());
        }

        // Drop the request receiver to stop processing
        self.request_rx.write().await.take();

        // Mark as not running
        *self.is_running.write().await = false;

        info!("ClientService stopped");
        Ok(())
    }

    async fn is_running(&self) -> bool {
        *self.is_running.read().await
    }

    async fn health_check(&self) -> ConsensusResult<ServiceHealth> {
        use crate::foundation::traits::HealthStatus;

        // Check if required services are available
        let has_global = self.global_consensus.read().await.is_some();
        let has_group = self.group_consensus.read().await.is_some();
        let has_routing = self.routing_service.read().await.is_some();

        let (status, message) = if !has_global || !has_group || !has_routing {
            (
                HealthStatus::Unhealthy,
                Some("Required services not available".to_string()),
            )
        } else if !self.is_running().await {
            (
                HealthStatus::Degraded,
                Some("Service not running".to_string()),
            )
        } else {
            (HealthStatus::Healthy, None)
        };

        Ok(ServiceHealth {
            name: "ClientService".to_string(),
            status,
            message,
            subsystems: vec![],
        })
    }
}
