//! Client service event subscriber for PubSub service
//!
//! Handles ClientServiceEvent requests that need PubSub functionality

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, error};

use crate::services::client::events::ClientServiceEvent;
use crate::services::event::{EventBus, EventHandler, EventPriority};
use crate::services::pubsub::{PubSubMessage, PubSubService, PubSubServiceEvent};
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

/// Subscriber for client service events that need PubSub functionality
#[derive(Clone)]
pub struct ClientServiceSubscriber<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    service: Arc<PubSubService<T, G>>,
    event_bus: Arc<EventBus>,
}

impl<T, G> ClientServiceSubscriber<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    /// Create a new client service subscriber
    pub fn new(service: Arc<PubSubService<T, G>>, event_bus: Arc<EventBus>) -> Self {
        Self { service, event_bus }
    }
}

#[async_trait]
impl<T, G> EventHandler<ClientServiceEvent> for ClientServiceSubscriber<T, G>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
{
    fn priority(&self) -> EventPriority {
        EventPriority::Normal
    }

    async fn handle(&self, event: ClientServiceEvent) {
        match event {
            ClientServiceEvent::PubSubPublish {
                request_id,
                subject,
                payload,
                headers,
            } => {
                debug!(
                    "ClientServiceSubscriber: Processing publish request for subject: {}",
                    subject
                );

                let result = self
                    .service
                    .publish(subject.to_string(), payload, headers)
                    .await;

                // Publish response event
                let response = match result {
                    Ok(()) => {
                        debug!(
                            "ClientServiceSubscriber: Publish completed for request {}",
                            request_id
                        );
                        PubSubServiceEvent::PublishComplete { request_id }
                    }
                    Err(e) => {
                        error!(
                            "ClientServiceSubscriber: Publish failed for request {}: {}",
                            request_id, e
                        );
                        PubSubServiceEvent::PublishError {
                            request_id,
                            error: e.to_string(),
                        }
                    }
                };
                self.event_bus.publish(response).await;
            }

            ClientServiceEvent::PubSubSubscribe {
                request_id,
                subject_pattern,
                queue_group,
                message_tx,
            } => {
                debug!(
                    "ClientServiceSubscriber: Processing subscribe request for pattern: {}",
                    subject_pattern
                );

                let result = self
                    .service
                    .subscribe(subject_pattern.as_ref().to_string(), queue_group)
                    .await;

                let response = match result {
                    Ok(subscription) => {
                        // Get receiver for this subscription
                        if let Some(mut receiver) = self
                            .service
                            .message_router()
                            .get_receiver(&subscription.id)
                            .await
                        {
                            // Spawn task to forward messages
                            let tx = message_tx.clone();
                            tokio::spawn(async move {
                                debug!("Starting message forwarding task for subscription");
                                while let Ok(msg) = receiver.recv().await {
                                    if let Err(e) = tx.send(PubSubMessage::from(msg)) {
                                        debug!("Failed to forward message: {:?}", e);
                                        break;
                                    }
                                }
                                debug!("Message forwarding task ended");
                            });
                        }

                        debug!(
                            "ClientServiceSubscriber: Subscribe completed for request {}, subscription id: {}",
                            request_id, subscription.id
                        );
                        PubSubServiceEvent::SubscribeComplete {
                            request_id,
                            subscription_id: subscription.id,
                        }
                    }
                    Err(e) => {
                        error!(
                            "ClientServiceSubscriber: Subscribe failed for request {}: {}",
                            request_id, e
                        );
                        PubSubServiceEvent::SubscribeError {
                            request_id,
                            error: e.to_string(),
                        }
                    }
                };
                self.event_bus.publish(response).await;
            }

            ClientServiceEvent::PubSubUnsubscribe {
                request_id,
                subscription_id,
            } => {
                debug!(
                    "ClientServiceSubscriber: Processing unsubscribe request for subscription: {}",
                    subscription_id
                );

                let result = self.service.unsubscribe(&subscription_id).await;

                let response = match result {
                    Ok(()) => {
                        debug!(
                            "ClientServiceSubscriber: Unsubscribe completed for request {}",
                            request_id
                        );
                        PubSubServiceEvent::UnsubscribeComplete { request_id }
                    }
                    Err(e) => {
                        error!(
                            "ClientServiceSubscriber: Unsubscribe failed for request {}: {}",
                            request_id, e
                        );
                        PubSubServiceEvent::UnsubscribeError {
                            request_id,
                            error: e.to_string(),
                        }
                    }
                };
                self.event_bus.publish(response).await;
            }

            // Ignore other client service events
            _ => {}
        }
    }
}
