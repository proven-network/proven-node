//! Network service handler for PubSub

use async_trait::async_trait;
use proven_network::{NetworkResult, Service, ServiceContext};
use std::sync::Arc;

use super::messages::{PubSubServiceMessage, PubSubServiceResponse};
use super::service::PubSubService;
use proven_attestation::Attestor;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;

/// PubSub service handler
pub struct PubSubHandler<T, G, A>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
{
    service: Arc<PubSubService<T, G, A>>,
}

impl<T, G, A> PubSubHandler<T, G, A>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
{
    /// Create a new handler
    pub fn new(service: Arc<PubSubService<T, G, A>>) -> Self {
        Self { service }
    }
}

#[async_trait]
impl<T, G, A> Service for PubSubHandler<T, G, A>
where
    T: Transport + Send + Sync + 'static,
    G: TopologyAdaptor + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    type Request = PubSubServiceMessage;

    async fn handle(
        &self,
        message: Self::Request,
        ctx: ServiceContext,
    ) -> NetworkResult<<Self::Request as proven_network::ServiceMessage>::Response> {
        match message {
            PubSubServiceMessage::Notify { messages } => {
                use super::types::PubSubMessageType;
                use crate::services::pubsub::types::PubSubNetworkMessage;
                use tracing::debug;

                // Handle batch of message notifications
                for msg in messages {
                    debug!(
                        "Processing message {} on subject {} from {}",
                        msg.id, msg.subject, msg.source
                    );

                    // Convert to internal format and route
                    // Add subject to headers if not already present
                    let mut headers = msg.headers;
                    if !headers.iter().any(|(k, _)| k == "subject") {
                        headers.push(("subject".to_string(), msg.subject.as_str().to_string()));
                    }

                    let network_msg = PubSubNetworkMessage {
                        id: msg.id,
                        payload: msg.payload,
                        headers,
                        timestamp: msg.timestamp,
                        source: msg.source,
                        msg_type: PubSubMessageType::Publish,
                    };

                    self.service.route_network_message(&network_msg).await;
                }
                Ok(PubSubServiceResponse::Ack)
            }
            PubSubServiceMessage::RegisterInterest {
                patterns,
                timestamp: _,
            } => {
                use tracing::debug;

                // Update interest tracker with peer's interests
                debug!(
                    "Received interest registration from {} with {} patterns",
                    ctx.sender,
                    patterns.len()
                );
                for pattern in &patterns {
                    debug!(
                        "Adding interest from {} for pattern {}",
                        ctx.sender,
                        pattern.as_ref()
                    );
                    self.service
                        .add_node_interest(ctx.sender.clone(), pattern.clone())
                        .await;
                }
                Ok(PubSubServiceResponse::Ack)
            }
            PubSubServiceMessage::UnregisterInterest {
                patterns,
                timestamp: _,
            } => {
                // Remove interests from tracker
                for pattern in patterns {
                    self.service
                        .remove_node_interest(ctx.sender.clone(), &pattern)
                        .await;
                }
                Ok(PubSubServiceResponse::Ack)
            }
        }
    }
}
