//! Command handlers for client service requests
//!
//! Handles request-response commands from the client service

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, error};

use crate::foundation::events::{Error, EventBus, EventMetadata, RequestHandler};
use crate::services::pubsub::commands::{PublishMessage, Subscribe, Unsubscribe};
use crate::services::pubsub::{PubSubMessage, PubSubService};
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

/// Handler for PublishMessage command
#[derive(Clone)]
pub struct PublishMessageHandler<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    service: Arc<PubSubService<T, G>>,
}

impl<T, G> PublishMessageHandler<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    pub fn new(service: Arc<PubSubService<T, G>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G> RequestHandler<PublishMessage> for PublishMessageHandler<T, G>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
{
    async fn handle(&self, request: PublishMessage, _metadata: EventMetadata) -> Result<(), Error> {
        debug!(
            "PublishMessageHandler: Processing publish request for subject: {}",
            request.subject
        );

        self.service
            .publish(
                request.subject.to_string(),
                request.payload,
                request.headers,
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to publish message: {e}")))
    }
}

/// Handler for Subscribe command
#[derive(Clone)]
pub struct SubscribeHandler<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    service: Arc<PubSubService<T, G>>,
}

impl<T, G> SubscribeHandler<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    pub fn new(service: Arc<PubSubService<T, G>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G> RequestHandler<Subscribe> for SubscribeHandler<T, G>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
{
    async fn handle(&self, request: Subscribe, _metadata: EventMetadata) -> Result<String, Error> {
        debug!(
            "SubscribeHandler: Processing subscribe request for pattern: {}",
            request.subject_pattern
        );

        let (subscription, rx) = self
            .service
            .subscribe(
                request.subject_pattern.as_ref().to_string(),
                request.queue_group,
            )
            .await
            .map_err(|e| Error::Internal(format!("Failed to subscribe: {e}")))?;

        // Spawn task to forward messages from the streaming receiver
        let tx = request.message_tx.clone();
        tokio::spawn(async move {
            debug!("Starting message forwarding task for subscription");
            while let Ok(msg) = rx.recv_async().await {
                if let Err(e) = tx.send(msg) {
                    debug!("Failed to forward message: {:?}", e);
                    break;
                }
            }
            debug!("Message forwarding task ended");
        });

        debug!(
            "SubscribeHandler: Subscribe completed, subscription id: {}",
            subscription.id
        );

        Ok(subscription.id)
    }
}

/// Handler for Unsubscribe command
#[derive(Clone)]
pub struct UnsubscribeHandler<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    service: Arc<PubSubService<T, G>>,
}

impl<T, G> UnsubscribeHandler<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    pub fn new(service: Arc<PubSubService<T, G>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G> RequestHandler<Unsubscribe> for UnsubscribeHandler<T, G>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
{
    async fn handle(&self, request: Unsubscribe, _metadata: EventMetadata) -> Result<(), Error> {
        debug!(
            "UnsubscribeHandler: Processing unsubscribe request for subscription: {}",
            request.subscription_id
        );

        self.service
            .unsubscribe(&request.subscription_id)
            .await
            .map_err(|e| Error::Internal(format!("Failed to unsubscribe: {e}")))
    }
}
