//! Network service handler for membership

use async_trait::async_trait;
use proven_network::{NetworkResult, Service, ServiceContext};
use std::sync::Arc;

use super::messages::MembershipMessage;
use super::service::MembershipService;
use proven_attestation::Attestor;
use proven_storage::StorageAdaptor;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

/// Membership service handler
pub struct MembershipHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    service: Arc<MembershipService<T, G, A, S>>,
}

impl<T, G, A, S> MembershipHandler<T, G, A, S>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
    S: StorageAdaptor,
{
    /// Create a new handler
    pub fn new(service: Arc<MembershipService<T, G, A, S>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G, A, S> Service for MembershipHandler<T, G, A, S>
where
    T: Transport + Send + Sync + 'static,
    G: TopologyAdaptor + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
    S: StorageAdaptor + Send + Sync + 'static,
{
    type Request = MembershipMessage;

    async fn handle(
        &self,
        message: Self::Request,
        ctx: ServiceContext,
    ) -> NetworkResult<<Self::Request as proven_network::ServiceMessage>::Response> {
        self.service.handle_message(ctx.sender, message).await
    }
}
