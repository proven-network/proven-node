//! Handler for PublishMessage command

use std::time::SystemTime;

use async_trait::async_trait;
use proven_topology::NodeId;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::foundation::events::{Error, EventMetadata, RequestHandler};
use crate::services::pubsub::commands::PublishMessage;
use crate::services::pubsub::interest::InterestTracker;
use crate::services::pubsub::internal::StreamManager;
use crate::services::pubsub::streaming_router::StreamingMessageRouter;

/// Handler for PublishMessage command
#[derive(Clone)]
pub struct PublishHandler {
    node_id: NodeId,
    max_message_size: usize,
    interest_tracker: InterestTracker,
    message_router: StreamingMessageRouter,
    stream_manager: StreamManager,
}

impl PublishHandler {
    pub fn new(
        node_id: NodeId,
        max_message_size: usize,
        interest_tracker: InterestTracker,
        message_router: StreamingMessageRouter,
        stream_manager: StreamManager,
    ) -> Self {
        Self {
            node_id,
            max_message_size,
            interest_tracker,
            message_router,
            stream_manager,
        }
    }
}

#[async_trait]
impl RequestHandler<PublishMessage> for PublishHandler {
    async fn handle(&self, request: PublishMessage, _metadata: EventMetadata) -> Result<(), Error> {
        debug!(
            "PublishMessageHandler: Processing {} messages for subject: {}",
            request.messages.len(),
            request.subject
        );

        // Validate subject
        let subject_string = request.subject.as_ref().to_string();

        // Process each message
        for mut message in request.messages {
            // Check message size
            if message.payload.len() > self.max_message_size {
                return Err(Error::Internal(format!(
                    "Message too large: {} > {}",
                    message.payload.len(),
                    self.max_message_size
                )));
            }

            // Add metadata headers if not present
            if message.get_header("message_id").is_none() {
                message = message.with_header("message_id", Uuid::new_v4().to_string());
            }
            if message.get_header("timestamp").is_none() {
                message = message.with_header(
                    "timestamp",
                    humantime::format_rfc3339(SystemTime::now()).to_string(),
                );
            }
            if message.get_header("source").is_none() {
                message = message.with_header("source", self.node_id.to_string());
            }

            // Route locally first
            let local_count = self.message_router.route(&message).await;
            debug!("Routed message to {} local subscribers", local_count);

            // Get interested nodes
            let interested_nodes = self
                .interest_tracker
                .find_interested_nodes(&subject_string)
                .await;

            if !interested_nodes.is_empty() {
                debug!(
                    "Sending message to {} interested nodes via streams",
                    interested_nodes.len()
                );

                // Get publish streams
                let publish_streams = self.stream_manager.get_publish_streams();
                let streams = publish_streams.read().await;

                info!(
                    "Publishing to {} interested nodes, we have {} publish streams",
                    interested_nodes.len(),
                    streams.len()
                );

                for node_id in interested_nodes {
                    if node_id != self.node_id {
                        if let Some(stream) = streams.get(&node_id) {
                            debug!("Sending message to {} via stream", node_id);
                            // Serialize and send via stream
                            let mut buf = Vec::new();
                            if let Err(e) = ciborium::into_writer(&message, &mut buf) {
                                warn!("Failed to serialize message: {}", e);
                                continue;
                            }

                            if let Err(e) = stream.send(buf).await {
                                warn!("Failed to send message to {} via stream: {}", node_id, e);
                            } else {
                                debug!("Successfully sent message to {} via stream", node_id);
                            }
                        } else {
                            warn!(
                                "No publish stream available for interested node {}",
                                node_id
                            );
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
