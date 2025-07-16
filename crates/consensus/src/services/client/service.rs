//! Client service implementation
//!
//! The ClientService acts as a facade for client operations, coordinating
//! with the global and group consensus services through direct service references
//! rather than the event bus.

use std::sync::Arc;

use tokio::sync::{RwLock, mpsc, oneshot};
use tracing::{debug, info};

use crate::{
    consensus::{
        global::{GlobalRequest, GlobalResponse},
        group::{GroupRequest, GroupResponse},
    },
    error::{ConsensusError, ConsensusResult},
    foundation::{
        traits::{ServiceHealth, ServiceLifecycle},
        types::ConsensusGroupId,
    },
    services::{
        global_consensus::GlobalConsensusService, group_consensus::GroupConsensusService,
        routing::RoutingService,
    },
};

use super::types::*;

/// Client service for handling client requests
pub struct ClientService<T, G, L>
where
    T: proven_transport::Transport,
    G: proven_governance::Governance,
    L: proven_storage::LogStorage,
{
    /// Service name
    name: String,

    /// Request receiver
    request_rx: Arc<RwLock<Option<mpsc::Receiver<ClientRequest>>>>,

    /// Request sender (for submitting requests)
    request_tx: mpsc::Sender<ClientRequest>,

    /// Global consensus service (set after construction)
    global_consensus: Arc<RwLock<Option<Arc<GlobalConsensusService<T, G, L>>>>>,

    /// Group consensus service (set after construction)
    group_consensus: Arc<RwLock<Option<Arc<GroupConsensusService<T, G, L>>>>>,

    /// Routing service (set after construction)
    routing_service: Arc<RwLock<Option<Arc<RoutingService>>>>,

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
    G: proven_governance::Governance + 'static,
    L: proven_storage::LogStorage + 'static,
{
    /// Create a new client service
    pub fn new() -> Self {
        let (request_tx, request_rx) = mpsc::channel(1000);

        Self {
            name: "ClientService".to_string(),
            request_rx: Arc::new(RwLock::new(Some(request_rx))),
            request_tx,
            global_consensus: Arc::new(RwLock::new(None)),
            group_consensus: Arc::new(RwLock::new(None)),
            routing_service: Arc::new(RwLock::new(None)),
            is_running: Arc::new(RwLock::new(false)),
            shutdown: Arc::new(RwLock::new(None)),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Set the global consensus service reference
    pub fn set_global_consensus(&self, service: Arc<GlobalConsensusService<T, G, L>>) {
        *self.global_consensus.blocking_write() = Some(service);
    }

    /// Set the group consensus service reference
    pub fn set_group_consensus(&self, service: Arc<GroupConsensusService<T, G, L>>) {
        *self.group_consensus.blocking_write() = Some(service);
    }

    /// Set the routing service reference
    pub fn set_routing_service(&self, service: Arc<RoutingService>) {
        *self.routing_service.blocking_write() = Some(service);
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

        tokio::spawn(async move {
            while let Some(request) = request_rx.recv().await {
                match request {
                    ClientRequest::Global {
                        request,
                        response_tx,
                    } => {
                        debug!("Processing global request: {:?}", request);

                        // Get global consensus service
                        let global = global_consensus.read().await;
                        if let Some(_service) = global.as_ref() {
                            // TODO: Submit directly to global consensus
                            // let operation = GlobalOperation::new(request);
                            // match service.submit_operation(operation).await {
                            //     Ok(response) => {
                            //         let _ = response_tx.send(Ok(response));
                            //     }
                            //     Err(e) => {
                            //         let _ = response_tx.send(Err(e));
                            //     }
                            // }
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
                        if let Some(_service) = group.as_ref() {
                            // TODO: Submit to specific group
                            // let operation = GroupOperation::new(request);
                            // match service.submit_to_group(group_id, operation).await {
                            //     Ok(response) => {
                            //         let _ = response_tx.send(Ok(response));
                            //     }
                            //     Err(e) => {
                            //         let _ = response_tx.send(Err(e));
                            //     }
                            // }
                            let _ = response_tx.send(Err(ConsensusError::with_context(
                                crate::error::ErrorKind::Internal,
                                "Group consensus operation submission not yet implemented",
                            )));
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
                        if let Some(_service) = routing.as_ref() {
                            // TODO: Get stream info from routing service
                            // match service.get_stream_info(&stream_name).await {
                            //     Ok(info) => {
                            //         let _ = response_tx.send(Ok(info));
                            //     }
                            //     Err(e) => {
                            //         let _ = response_tx.send(Err(e));
                            //     }
                            // }
                            let _ = response_tx.send(Err(ConsensusError::with_context(
                                crate::error::ErrorKind::Internal,
                                "Stream info query not yet implemented",
                            )));
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
                }
            }

            info!("Client request processor stopped");
        });
    }
}

#[async_trait::async_trait]
impl<T, G, L> ServiceLifecycle for ClientService<T, G, L>
where
    T: proven_transport::Transport + 'static,
    G: proven_governance::Governance + 'static,
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
