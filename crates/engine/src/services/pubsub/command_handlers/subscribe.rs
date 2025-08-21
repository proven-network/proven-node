//! Handler for streaming subscriptions

use async_trait::async_trait;
use std::time::SystemTime;
use tracing::{debug, info, warn};

use crate::foundation::Message;
use crate::foundation::events::{Error as EventError, EventMetadata, StreamHandler};
use crate::foundation::types::SubjectPattern;
use crate::services::pubsub::interest::InterestTracker;
use crate::services::pubsub::internal::InterestPropagator;
use crate::services::pubsub::streaming_router::StreamingMessageRouter;
use crate::services::pubsub::types::Subscription;
use crate::services::pubsub::{Subscribe, SubscriptionControl};
use proven_topology::NodeId;

/// Handler for subscriptions
pub struct SubscribeHandler {
    node_id: NodeId,
    max_subscriptions_per_node: usize,
    interest_tracker: InterestTracker,
    message_router: StreamingMessageRouter,
    interest_propagator: std::sync::Arc<InterestPropagator>,
}

impl SubscribeHandler {
    pub fn new(
        node_id: NodeId,
        max_subscriptions_per_node: usize,
        interest_tracker: InterestTracker,
        message_router: StreamingMessageRouter,
        interest_propagator: std::sync::Arc<InterestPropagator>,
    ) -> Self {
        Self {
            node_id,
            max_subscriptions_per_node,
            interest_tracker,
            message_router,
            interest_propagator,
        }
    }
}

#[async_trait]
impl StreamHandler<Subscribe> for SubscribeHandler {
    async fn handle(
        &self,
        request: Subscribe,
        _metadata: EventMetadata,
        sink: flume::Sender<Message>,
    ) -> Result<(), EventError> {
        debug!(
            "Creating streaming subscription for pattern: {:?} (queue_group: {:?})",
            request.subject_pattern, request.queue_group
        );

        // Validate pattern
        let pattern = SubjectPattern::new(request.subject_pattern.as_ref())
            .map_err(|e| EventError::Internal(format!("Invalid pattern: {e}")))?;

        // Check subscription limit
        let current_count = self.message_router.get_subscriptions().await.len();
        if current_count >= self.max_subscriptions_per_node {
            return Err(EventError::Internal(format!(
                "Maximum subscriptions reached: {}",
                self.max_subscriptions_per_node
            )));
        }

        // Create subscription
        let subscription = Subscription {
            id: uuid::Uuid::new_v4().to_string(),
            subject_pattern: pattern.clone(),
            node_id: self.node_id,
            queue_group: request.queue_group.clone(),
            created_at: SystemTime::now(),
        };

        let subscription_id = subscription.id.clone();

        // Create channel for message delivery
        let (tx, rx) = flume::unbounded();

        // Add to router
        self.message_router
            .add_streaming_subscription(subscription.id.clone(), subscription.clone(), tx)
            .await;

        // Update interest
        self.interest_tracker
            .add_interest(self.node_id, pattern.clone())
            .await;

        // Propagate interests to all connected peers
        debug!(
            "Propagating interests after new subscription to pattern: {}",
            pattern
        );
        if let Err(e) = self.interest_propagator.propagate_interests().await {
            warn!("Failed to propagate interests: {}", e);
        } else {
            debug!("Successfully propagated interests");
        }

        // Create control channel for subscription management
        let (_control_tx, control_rx) = flume::bounded(10);

        // Spawn a task to handle control messages
        let message_router = self.message_router.clone();
        let interest_tracker = self.interest_tracker.clone();
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
                        debug!(
                            "Unsubscribing streaming subscription {}",
                            subscription_id_clone
                        );

                        // Get subscription info before removing
                        let subscription = message_router
                            .get_subscription(&subscription_id_clone)
                            .await;

                        // Remove from router
                        message_router
                            .remove_subscription(&subscription_id_clone)
                            .await;

                        // Update interest if we got the pattern
                        if let Some(subscription) = subscription {
                            // Check if there are any other subscriptions for this pattern
                            let all_subs = message_router.get_subscriptions().await;
                            let remaining = all_subs
                                .iter()
                                .filter(|s| {
                                    s.subject_pattern == subscription.subject_pattern
                                        && s.id != subscription.id
                                })
                                .count();

                            if remaining == 0 {
                                interest_tracker
                                    .remove_interest(
                                        subscription.node_id,
                                        &subscription.subject_pattern,
                                    )
                                    .await;
                            }
                        }

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
                // Messages are already Message type
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

        debug!(
            "Streaming subscription {} created successfully",
            subscription_id
        );

        // Note: We're not returning a handle here because the StreamHandler
        // doesn't support returning values. The client will manage the stream
        // through the control channel if needed.

        Ok(())
    }
}
