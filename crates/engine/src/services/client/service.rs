//! Client service implementation
//!
//! The ClientService acts as a facade for client operations, coordinating
//! with the global and group consensus services through direct service references
//! rather than the event bus.

use std::sync::Arc;

use crate::services::stream::StreamStorageReader;
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
        event::Event,
        global_consensus::GlobalConsensusService,
        group_consensus::GroupConsensusService,
        routing::{GroupRoute, RoutingService},
    },
};

use super::types::*;

/// Type alias for optional service references
type OptionalServiceRef<T> = Arc<RwLock<Option<Arc<T>>>>;
type OptionalNetworkManager<T, G> = Arc<RwLock<Option<Arc<proven_network::NetworkManager<T, G>>>>>;

/// Client service for handling client requests
pub struct ClientService<T, G, L>
where
    T: proven_transport::Transport,
    G: proven_topology::TopologyAdaptor,
    L: proven_storage::LogStorageWithDelete,
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
    stream_service: OptionalServiceRef<crate::services::stream::StreamService<L>>,

    /// Event service (set after construction)
    event_service: Arc<RwLock<Option<Arc<crate::services::event::EventService>>>>,

    /// Network manager (set after construction, for forwarding requests)
    network_manager: OptionalNetworkManager<T, G>,

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
    L: proven_storage::LogStorageWithDelete + 'static,
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
            network_manager: Arc::new(RwLock::new(None)),
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
    pub async fn set_stream_service(
        &self,
        service: Arc<crate::services::stream::StreamService<L>>,
    ) {
        *self.stream_service.write().await = Some(service);
    }

    /// Set the event service reference
    pub async fn set_event_service(&self, service: Arc<crate::services::event::EventService>) {
        *self.event_service.write().await = Some(service);
    }

    /// Set the network manager reference
    pub async fn set_network_manager(&self, network: Arc<proven_network::NetworkManager<T, G>>) {
        *self.network_manager.write().await = Some(network);
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

    /// Get a suitable group for stream creation
    ///
    /// This method finds an appropriate group that this node is a member of.
    /// It prioritizes:
    /// 1. Groups where this node is a member
    /// 2. The default group (ID 1) if it exists and node is a member
    /// 3. The least loaded group the node belongs to
    pub async fn get_suitable_group(&self) -> ConsensusResult<ConsensusGroupId> {
        // Get routing service to check group membership
        let routing_service = self.routing_service.read().await;
        let routing_service = routing_service.as_ref().ok_or_else(|| {
            ConsensusError::with_context(
                crate::error::ErrorKind::Service,
                "Routing service not available",
            )
        })?;

        // Get all routing info
        let routing_info = routing_service.get_routing_info().await.map_err(|e| {
            ConsensusError::with_context(
                crate::error::ErrorKind::Service,
                format!("Failed to get routing info: {e}"),
            )
        })?;

        // Find groups that are local to this node
        let mut local_groups: Vec<(&ConsensusGroupId, &GroupRoute)> = routing_info
            .group_routes
            .iter()
            .filter(|(_, group)| {
                group.location == crate::services::routing::GroupLocation::Local
                    || group.location == crate::services::routing::GroupLocation::Distributed
            })
            .collect();

        if local_groups.is_empty() {
            // No local groups - try to find a remote group to forward to
            let remote_groups: Vec<(&ConsensusGroupId, &GroupRoute)> = routing_info
                .group_routes
                .iter()
                .filter(|(_, group)| {
                    group.location == crate::services::routing::GroupLocation::Remote
                })
                .collect();
            if remote_groups.is_empty() {
                return Err(ConsensusError::with_context(
                    crate::error::ErrorKind::InvalidState,
                    "No consensus groups available",
                ));
            }
            // For now, return an error indicating we need to forward
            // In the future, this should return information about which node to forward to
            return Err(ConsensusError::with_context(
                crate::error::ErrorKind::InvalidState,
                "All groups are remote - request forwarding not yet implemented",
            ));
        }

        // Sort by preference:
        // 1. Default group (ID 1) comes first
        // 2. Then by stream count (least loaded)
        local_groups.sort_by(|(id_a, group_a), (id_b, group_b)| {
            // Check if either is the default group
            let default_id = ConsensusGroupId::new(1);
            if **id_a == default_id && **id_b != default_id {
                return std::cmp::Ordering::Less;
            }
            if **id_b == default_id && **id_a != default_id {
                return std::cmp::Ordering::Greater;
            }

            // Otherwise sort by stream count (ascending)
            group_a.stream_count.cmp(&group_b.stream_count)
        });

        // Return the best group
        Ok(*local_groups[0].0)
    }

    /// Read messages directly from a stream (bypasses consensus)
    pub async fn read_stream(
        &self,
        stream_name: &str,
        start_sequence: u64,
        count: u64,
    ) -> ConsensusResult<Vec<crate::services::stream::StoredMessage>> {
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
        let _event_service = self.event_service.clone();
        let node_id = self.node_id.clone();
        let network_manager = self.network_manager.clone();

        // Create an Arc reference to self for forwarding
        let client_service = Arc::new(ClientService {
            name: self.name.clone(),
            node_id: self.node_id.clone(),
            request_rx: Arc::new(RwLock::new(None)), // Not used in forwarding
            request_tx: self.request_tx.clone(),
            global_consensus: self.global_consensus.clone(),
            group_consensus: self.group_consensus.clone(),
            routing_service: self.routing_service.clone(),
            stream_service: self.stream_service.clone(),
            event_service: self.event_service.clone(),
            network_manager: self.network_manager.clone(),
            is_running: self.is_running.clone(),
            shutdown: self.shutdown.clone(),
            _phantom: std::marker::PhantomData,
        });

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
                                                // Determine if this node is part of the group
                                                let location = if info.members.contains(&node_id) {
                                                    crate::services::routing::GroupLocation::Local
                                                } else {
                                                    crate::services::routing::GroupLocation::Remote
                                                };
                                                let group_route = crate::services::routing::GroupRoute {
                                                    group_id: info.id,
                                                    members: info.members.clone(),
                                                    leader: info.members.first().cloned(),
                                                    stream_count: 0,
                                                    health: crate::services::routing::GroupHealth::Healthy,
                                                    last_updated: std::time::SystemTime::now(),
                                                    location,
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
                            GlobalRequest::CreateStream { .. } => {
                                // Stream creation goes through global consensus
                                let global = global_consensus.read().await;
                                if let Some(service) = global.as_ref() {
                                    match service.submit_request(request.clone()).await {
                                        Ok(response) => {
                                            // Check if the response is an error variant
                                            match &response {
                                                GlobalResponse::Error { message } => {
                                                    let _ =
                                                        response_tx
                                                            .send(Err(ConsensusError::with_context(
                                                            crate::error::ErrorKind::InvalidState,
                                                            message.clone(),
                                                        )));
                                                }
                                                _ => {
                                                    let _ = response_tx.send(Ok(response));
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            // Check if we need to forward to the leader
                                            let error_str = e.to_string();
                                            if error_str.contains("has to forward request to") {
                                                // Try to get the global leader from routing service first
                                                let routing = routing_service.read().await;
                                                let mut leader_id = None;
                                                if let Some(routing) = routing.as_ref() {
                                                    leader_id = routing.get_global_leader().await;
                                                }
                                                // If routing service doesn't know the leader, log a warning
                                                // In a future version, we could parse the leader from the error message
                                                if leader_id.is_none() {
                                                    debug!(
                                                        "Routing service doesn't know global leader yet, cannot forward request"
                                                    );
                                                }
                                                if let Some(leader_id) = leader_id {
                                                    if leader_id != node_id {
                                                        // Forward to the leader
                                                        debug!(
                                                            "Forwarding global request to leader: {}",
                                                            leader_id
                                                        );
                                                        // Create forward request
                                                        let forward_request =
                                                            super::messages::ClientServiceMessage::ForwardGlobalRequest {
                                                                requester_id: node_id.clone(),
                                                                request: request.clone(),
                                                            };
                                                        // Send to leader
                                                        let network = network_manager.read().await;
                                                        if let Some(network) = network.as_ref() {
                                                            match network
                                                                .request_service(
                                                                    leader_id.clone(),
                                                                    forward_request,
                                                                    std::time::Duration::from_secs(30),
                                                                )
                                                                .await
                                                            {
                                                                Ok(super::messages::ClientServiceResponse::ForwardGlobalResponse { response }) => {
                                                                    // Check if the forwarded response is an error variant
                                                                    match &response {
                                                                        GlobalResponse::Error { message } => {
                                                                            let _ = response_tx.send(Err(ConsensusError::with_context(
                                                                                crate::error::ErrorKind::InvalidState,
                                                                                message.clone(),
                                                                            )));
                                                                        }
                                                                        _ => {
                                                                            let _ = response_tx.send(Ok(response));
                                                                        }
                                                                    }
                                                                }
                                                                Ok(super::messages::ClientServiceResponse::ForwardGroupResponse { .. }) => {
                                                                    let _ = response_tx.send(Err(
                                                                        ConsensusError::with_context(
                                                                            crate::error::ErrorKind::Internal,
                                                                            "Unexpected response type: expected ForwardGlobalResponse",
                                                                        ),
                                                                    ));
                                                                }
                                                                Err(e) => {
                                                                    let _ = response_tx.send(Err(
                                                                        ConsensusError::with_context(
                                                                            crate::error::ErrorKind::Network,
                                                                            format!("Failed to forward request: {e}"),
                                                                        ),
                                                                    ));
                                                                }
                                                            }
                                                        } else {
                                                            let _ = response_tx.send(Err(
                                                                ConsensusError::with_context(
                                                                    crate::error::ErrorKind::Service,
                                                                    "Network manager not available",
                                                                ),
                                                            ));
                                                        }
                                                    } else {
                                                        // We are the leader but still got an error
                                                        let _ = response_tx.send(Err(e));
                                                    }
                                                } else {
                                                    // No leader known, return original error
                                                    let _ = response_tx.send(Err(e));
                                                }
                                            } else {
                                                let _ = response_tx.send(Err(e));
                                            }
                                        }
                                    }
                                } else {
                                    let _ = response_tx.send(Err(ConsensusError::with_context(
                                        crate::error::ErrorKind::Service,
                                        "Global consensus service not available",
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
                                                        crate::services::stream::StreamConfig {
                                                            max_message_size: 1024 * 1024,
                                                            retention:
                                                                crate::services::stream::config::RetentionPolicy::Forever,
                                                            persistence_type:
                                                                crate::services::stream::config::PersistenceType::Persistent,
                                                            allow_auto_create: false,
                                                        }
                                                    }),
                                                    group_id: route.group_id,
                                                    last_sequence: stream_state.next_sequence.saturating_sub(1),
                                                    message_count: stream_state.stats.message_count,
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

                                    // Check if the group is local or remote
                                    match routing_service.is_group_local(group_id).await {
                                        Ok(true) => {
                                            // Group is local, submit directly
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
                                        }
                                        Ok(false) => {
                                            // Group is remote, need to forward
                                            if let Ok(Some(group_info)) = routing_service
                                                .get_routing_info()
                                                .await
                                                .map(|info| {
                                                    info.group_routes.get(&group_id).cloned()
                                                })
                                            {
                                                match client_service
                                                    .forward_to_group(
                                                        group_id,
                                                        &group_info.members,
                                                        request,
                                                    )
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
                                                        crate::error::ErrorKind::InvalidState,
                                                        format!(
                                                            "No routing info for group {group_id:?}"
                                                        ),
                                                    ),
                                                ));
                                            }
                                        }
                                        Err(e) => {
                                            let _ =
                                                response_tx.send(Err(ConsensusError::with_context(
                                                    crate::error::ErrorKind::NotFound,
                                                    format!(
                                                        "Group {group_id:?} not found in routing table: {e}"
                                                    ),
                                                )));
                                        }
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
        request: GroupRequest,
    ) -> ConsensusResult<GroupResponse> {
        let network = self.network_manager.read().await;
        let network = network.as_ref().ok_or_else(|| {
            ConsensusError::with_context(
                crate::error::ErrorKind::Configuration,
                "Network manager not available for forwarding",
            )
        })?;

        // Get routing service to find the best node
        let routing_service = self.routing_service.read().await;
        let routing_service = routing_service.as_ref().ok_or_else(|| {
            ConsensusError::with_context(
                crate::error::ErrorKind::Service,
                "Routing service not available",
            )
        })?;

        // Get the best node to forward to
        let target_node = routing_service
            .get_best_node_for_group(group_id)
            .await
            .map_err(|e| {
                ConsensusError::with_context(
                    crate::error::ErrorKind::Service,
                    format!("Failed to get target node for group {group_id:?}: {e}"),
                )
            })?;

        let target_node = target_node
            .or_else(|| members.first().cloned())
            .ok_or_else(|| {
                ConsensusError::with_context(
                    crate::error::ErrorKind::InvalidState,
                    format!("No target node available for group {group_id:?}"),
                )
            })?;

        // Create the forward request
        let forward_request = super::messages::ClientServiceMessage::ForwardGroupRequest {
            requester_id: self.node_id.clone(),
            group_id,
            request,
        };

        // Send the request and wait for response
        let response = network
            .request_service(
                target_node.clone(),
                forward_request,
                std::time::Duration::from_secs(30),
            )
            .await
            .map_err(|e| {
                ConsensusError::with_context(
                    crate::error::ErrorKind::Network,
                    format!("Failed to forward request to {target_node}: {e}"),
                )
            })?;

        match response {
            super::messages::ClientServiceResponse::ForwardGroupResponse { response } => {
                Ok(response)
            }
            _ => Err(ConsensusError::with_context(
                crate::error::ErrorKind::Internal,
                "Unexpected response type",
            )),
        }
    }

    /// Register network handlers for forwarded requests
    async fn register_network_handlers(
        &self,
        network: Arc<proven_network::NetworkManager<T, G>>,
    ) -> ConsensusResult<()> {
        info!("Registering client service network handlers");

        // Clone references for handlers
        let node_id = self.node_id.clone();
        let group_consensus = self.group_consensus.clone();
        let global_consensus = self.global_consensus.clone();

        // Register service handler
        network
            .register_service::<super::messages::ClientServiceMessage, _>(move |sender, message| {
                let _node_id = node_id.clone();
                let group_consensus = group_consensus.clone();
                let global_consensus = global_consensus.clone();

                Box::pin(async move {
                    match message {
                        super::messages::ClientServiceMessage::ForwardGroupRequest {
                            requester_id: _,
                            group_id,
                            request,
                        } => {
                            tracing::info!(
                                "Received forwarded group request from {} for group {:?}",
                                sender,
                                group_id
                            );

                            // Get group consensus service
                            let service = group_consensus.read().await;
                            let service = service.as_ref().ok_or_else(|| {
                                proven_network::NetworkError::Internal(
                                    "Group consensus service not available".to_string(),
                                )
                            })?;

                            // Submit the request locally
                            let response = service
                                .submit_to_group(group_id, request)
                                .await
                                .map_err(|e| {
                                    proven_network::NetworkError::Internal(format!(
                                        "Failed to submit to group: {e}"
                                    ))
                                })?;

                            Ok(
                                super::messages::ClientServiceResponse::ForwardGroupResponse {
                                    response,
                                },
                            )
                        }
                        super::messages::ClientServiceMessage::ForwardGlobalRequest {
                            requester_id: _,
                            request,
                        } => {
                            tracing::info!("Received forwarded global request from {}", sender);

                            // Get global consensus service
                            let service = global_consensus.read().await;
                            let service = service.as_ref().ok_or_else(|| {
                                proven_network::NetworkError::Internal(
                                    "Global consensus service not available".to_string(),
                                )
                            })?;

                            // Submit the request locally
                            let response = service.submit_request(request).await.map_err(|e| {
                                proven_network::NetworkError::Internal(format!(
                                    "Failed to submit to global consensus: {e}"
                                ))
                            })?;
                            Ok(
                                super::messages::ClientServiceResponse::ForwardGlobalResponse {
                                    response,
                                },
                            )
                        }
                    }
                })
            })
            .await?;

        info!("Client service network handlers registered successfully");
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T, G, L> ServiceLifecycle for ClientService<T, G, L>
where
    T: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    L: proven_storage::LogStorageWithDelete + 'static,
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

        // Register network handlers if network manager is available
        if let Some(network) = self.network_manager.read().await.as_ref()
            && let Err(e) = self.register_network_handlers(network.clone()).await
        {
            tracing::warn!("Failed to register network handlers: {e}");
            // Continue anyway - the service can still work for local requests
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
