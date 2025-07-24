//! PubSub service event subscriber for client service
//!
//! Handles PubSubServiceEvent responses from the PubSub service

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, oneshot};
use tracing::{debug, error, warn};

use crate::services::event::{EventHandler, EventPriority};
use crate::services::pubsub::PubSubServiceEvent;
use uuid::Uuid;

/// Tracks pending PubSub requests
type PendingRequests = Arc<RwLock<HashMap<Uuid, PendingRequest>>>;

/// A pending request waiting for response
enum PendingRequest {
    Publish(oneshot::Sender<Result<(), String>>),
    Subscribe(oneshot::Sender<Result<String, String>>),
    Unsubscribe(oneshot::Sender<Result<(), String>>),
}

/// Subscriber for PubSub service events
#[derive(Clone)]
pub struct PubSubServiceSubscriber {
    /// Pending requests waiting for responses
    pending_requests: PendingRequests,
}

impl PubSubServiceSubscriber {
    /// Create a new PubSub service subscriber
    pub fn new() -> Self {
        Self {
            pending_requests: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a pending publish request
    pub async fn register_publish(
        &self,
        request_id: Uuid,
    ) -> oneshot::Receiver<Result<(), String>> {
        let (tx, rx) = oneshot::channel();
        self.pending_requests
            .write()
            .await
            .insert(request_id, PendingRequest::Publish(tx));
        rx
    }

    /// Register a pending subscribe request
    pub async fn register_subscribe(
        &self,
        request_id: Uuid,
    ) -> oneshot::Receiver<Result<String, String>> {
        let (tx, rx) = oneshot::channel();
        self.pending_requests
            .write()
            .await
            .insert(request_id, PendingRequest::Subscribe(tx));
        rx
    }

    /// Register a pending unsubscribe request
    pub async fn register_unsubscribe(
        &self,
        request_id: Uuid,
    ) -> oneshot::Receiver<Result<(), String>> {
        let (tx, rx) = oneshot::channel();
        self.pending_requests
            .write()
            .await
            .insert(request_id, PendingRequest::Unsubscribe(tx));
        rx
    }
}

impl Default for PubSubServiceSubscriber {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventHandler<PubSubServiceEvent> for PubSubServiceSubscriber {
    fn priority(&self) -> EventPriority {
        EventPriority::Normal
    }

    async fn handle(&self, event: PubSubServiceEvent) {
        match event {
            PubSubServiceEvent::MessageReceived { .. } => {
                // Message received events are handled by other subscribers
                // (e.g., stream service might listen for messages matching stream patterns)
            }

            PubSubServiceEvent::PublishComplete { request_id } => {
                debug!(
                    "PubSubServiceSubscriber: Received publish complete for request {}",
                    request_id
                );

                if let Some(PendingRequest::Publish(tx)) =
                    self.pending_requests.write().await.remove(&request_id)
                {
                    let _ = tx.send(Ok(()));
                } else {
                    warn!(
                        "Received PublishComplete for unknown request: {}",
                        request_id
                    );
                }
            }

            PubSubServiceEvent::PublishError { request_id, error } => {
                error!(
                    "PubSubServiceSubscriber: Received publish error for request {}: {}",
                    request_id, error
                );

                if let Some(PendingRequest::Publish(tx)) =
                    self.pending_requests.write().await.remove(&request_id)
                {
                    let _ = tx.send(Err(error));
                } else {
                    warn!("Received PublishError for unknown request: {}", request_id);
                }
            }

            PubSubServiceEvent::SubscribeComplete {
                request_id,
                subscription_id,
            } => {
                debug!(
                    "PubSubServiceSubscriber: Received subscribe complete for request {}, subscription: {}",
                    request_id, subscription_id
                );

                if let Some(PendingRequest::Subscribe(tx)) =
                    self.pending_requests.write().await.remove(&request_id)
                {
                    let _ = tx.send(Ok(subscription_id));
                } else {
                    warn!(
                        "Received SubscribeComplete for unknown request: {}",
                        request_id
                    );
                }
            }

            PubSubServiceEvent::SubscribeError { request_id, error } => {
                error!(
                    "PubSubServiceSubscriber: Received subscribe error for request {}: {}",
                    request_id, error
                );

                if let Some(PendingRequest::Subscribe(tx)) =
                    self.pending_requests.write().await.remove(&request_id)
                {
                    let _ = tx.send(Err(error));
                } else {
                    warn!(
                        "Received SubscribeError for unknown request: {}",
                        request_id
                    );
                }
            }

            PubSubServiceEvent::UnsubscribeComplete { request_id } => {
                debug!(
                    "PubSubServiceSubscriber: Received unsubscribe complete for request {}",
                    request_id
                );

                if let Some(PendingRequest::Unsubscribe(tx)) =
                    self.pending_requests.write().await.remove(&request_id)
                {
                    let _ = tx.send(Ok(()));
                } else {
                    warn!(
                        "Received UnsubscribeComplete for unknown request: {}",
                        request_id
                    );
                }
            }

            PubSubServiceEvent::UnsubscribeError { request_id, error } => {
                error!(
                    "PubSubServiceSubscriber: Received unsubscribe error for request {}: {}",
                    request_id, error
                );

                if let Some(PendingRequest::Unsubscribe(tx)) =
                    self.pending_requests.write().await.remove(&request_id)
                {
                    let _ = tx.send(Err(error));
                } else {
                    warn!(
                        "Received UnsubscribeError for unknown request: {}",
                        request_id
                    );
                }
            }
        }
    }
}
