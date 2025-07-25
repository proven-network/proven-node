//! Streaming handlers for the PubSub service

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, info};

use crate::foundation::events::{Error as EventError, EventMetadata, StreamHandler};
use crate::services::pubsub::{PubSubMessage, PubSubService, SubscribeStream, SubscriptionControl};
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

/// Handler for streaming subscriptions
pub struct SubscribeStreamHandler<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    pubsub_service: Arc<PubSubService<T, G>>,
}

impl<T, G> SubscribeStreamHandler<T, G>
where
    T: Transport,
    G: TopologyAdaptor,
{
    pub fn new(pubsub_service: Arc<PubSubService<T, G>>) -> Self {
        Self { pubsub_service }
    }
}

#[async_trait]
impl<T, G> StreamHandler<SubscribeStream> for SubscribeStreamHandler<T, G>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
{
    async fn handle(
        &self,
        request: SubscribeStream,
        _metadata: EventMetadata,
        sink: flume::Sender<PubSubMessage>,
    ) -> Result<(), EventError> {
        info!(
            "Creating streaming subscription for pattern: {:?} (queue_group: {:?})",
            request.subject_pattern, request.queue_group
        );

        // Create control channel for subscription management
        let (_control_tx, control_rx) = flume::bounded(10);

        // Use the PubSub service's subscribe method which returns a receiver
        let (subscription, rx) = self
            .pubsub_service
            .subscribe(
                request.subject_pattern.as_ref().to_string(),
                request.queue_group,
            )
            .await
            .map_err(|e| EventError::Internal(format!("Failed to subscribe: {e}")))?;

        let subscription_id = subscription.id.clone();

        // Spawn a task to handle control messages
        let pubsub_service = self.pubsub_service.clone();
        let subscription_id_clone = subscription_id.clone();

        tokio::spawn(async move {
            while let Ok(control) = control_rx.recv_async().await {
                match control {
                    SubscriptionControl::Pause => {
                        debug!("Pausing subscription {}", subscription_id_clone);
                        // TODO: Implement pause logic if needed
                    }
                    SubscriptionControl::Resume => {
                        debug!("Resuming subscription {}", subscription_id_clone);
                        // TODO: Implement resume logic if needed
                    }
                    SubscriptionControl::Unsubscribe => {
                        info!(
                            "Unsubscribing streaming subscription {}",
                            subscription_id_clone
                        );

                        // Call unsubscribe which handles cleanup
                        let _ = pubsub_service.unsubscribe(&subscription_id_clone).await;

                        // Exit the control loop
                        break;
                    }
                }
            }
            debug!(
                "Control handler for subscription {} exited",
                subscription_id_clone
            );
        });

        // Forward messages from the receiver to the sink
        let sub_id_for_task = subscription_id.clone();
        tokio::spawn(async move {
            while let Ok(msg) = rx.recv_async().await {
                // Messages are already PubSubMessage type
                if sink.send_async(msg).await.is_err() {
                    // Client disconnected
                    break;
                }
            }
            debug!(
                "Message forwarding task for subscription {} exited",
                sub_id_for_task
            );
        });

        info!(
            "Streaming subscription {} created successfully",
            subscription_id
        );

        // Note: We're not returning a handle here because the StreamHandler
        // doesn't support returning values. The client will manage the stream
        // through the control channel if needed.

        Ok(())
    }
}
