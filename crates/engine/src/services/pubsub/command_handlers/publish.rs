//! Handler for PublishMessage command

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use proven_attestation::Attestor;
use proven_network::NetworkManager;
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;
use tracing::debug;
use uuid::Uuid;

use crate::foundation::events::{Error, EventBus, EventMetadata, RequestHandler};
use crate::services::pubsub::commands::PublishMessage;
use crate::services::pubsub::interest::InterestTracker;
use crate::services::pubsub::messages::{MessageNotification, PubSubServiceMessage};
use crate::services::pubsub::streaming_router::StreamingMessageRouter;
use crate::services::pubsub::types::{PubSubMessageType, PubSubNetworkMessage};

/// Handler for PublishMessage command
#[derive(Clone)]
pub struct PublishHandler<T, G, A>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
{
    node_id: NodeId,
    max_message_size: usize,
    interest_tracker: InterestTracker,
    message_router: StreamingMessageRouter,
    network: Arc<NetworkManager<T, G, A>>,
    event_bus: Arc<EventBus>,
}

impl<T, G, A> PublishHandler<T, G, A>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
{
    pub fn new(
        node_id: NodeId,
        max_message_size: usize,
        interest_tracker: InterestTracker,
        message_router: StreamingMessageRouter,
        network: Arc<NetworkManager<T, G, A>>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            node_id,
            max_message_size,
            interest_tracker,
            message_router,
            network,
            event_bus,
        }
    }
}

#[async_trait]
impl<T, G, A> RequestHandler<PublishMessage> for PublishHandler<T, G, A>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    A: Attestor + 'static,
{
    async fn handle(&self, request: PublishMessage, _metadata: EventMetadata) -> Result<(), Error> {
        debug!(
            "PublishMessageHandler: Processing {} messages for subject: {}",
            request.messages.len(),
            request.subject
        );

        // Validate subject
        let subject_string = request.subject.as_ref().to_string();

        // Process each message
        for message in request.messages {
            // Check message size
            if message.payload.len() > self.max_message_size {
                return Err(Error::Internal(format!(
                    "Message too large: {} > {}",
                    message.payload.len(),
                    self.max_message_size
                )));
            }

            let id = Uuid::new_v4();

            // Create network message - message already has subject in headers
            let network_message = PubSubNetworkMessage {
                id,
                msg_type: PubSubMessageType::Publish,
                source: self.node_id.clone(),
                payload: message.payload.clone(),
                headers: message.headers.clone(),
                timestamp: SystemTime::now(),
            };

            // Route locally first
            let local_count = self.message_router.route(&network_message).await;
            debug!("Routed message to {} local subscribers", local_count);

            // Get interested nodes
            let interested_nodes = self
                .interest_tracker
                .find_interested_nodes(&subject_string)
                .await;

            if !interested_nodes.is_empty() {
                debug!(
                    "Broadcasting message to {} interested nodes",
                    interested_nodes.len()
                );

                for node_id in interested_nodes {
                    if node_id != self.node_id {
                        // Send message to interested node via PubSubServiceMessage
                        // Extract subject from headers - we know it exists because we validated it
                        let subject = crate::foundation::types::Subject::new(&subject_string)
                            .expect("Subject was already validated");

                        let notification = MessageNotification {
                            id: network_message.id,
                            subject,
                            payload: network_message.payload.clone(),
                            headers: network_message.headers.clone(),
                            timestamp: network_message.timestamp,
                            source: network_message.source.clone(),
                        };
                        let service_msg = PubSubServiceMessage::Notify {
                            messages: vec![notification],
                        };
                        if let Err(e) = self
                            .network
                            .request_with_timeout(
                                node_id.clone(),
                                service_msg,
                                std::time::Duration::from_secs(5),
                            )
                            .await
                        {
                            debug!("Failed to send message to {}: {}", node_id, e);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
