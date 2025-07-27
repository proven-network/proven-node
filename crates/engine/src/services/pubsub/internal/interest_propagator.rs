//! Interest propagation component for PubSub service

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::foundation::types::SubjectPattern;
use crate::services::pubsub::interest::InterestTracker;
use crate::services::pubsub::streaming::InterestMessage;
use proven_network::Stream;
use proven_topology::NodeId;

/// Handles propagation of interests to peer nodes
pub struct InterestPropagator {
    node_id: NodeId,
    interest_tracker: InterestTracker,
    control_streams: Arc<RwLock<HashMap<NodeId, Stream>>>,
}

impl InterestPropagator {
    pub fn new(
        node_id: NodeId,
        interest_tracker: InterestTracker,
        control_streams: Arc<RwLock<HashMap<NodeId, Stream>>>,
    ) -> Self {
        Self {
            node_id,
            interest_tracker,
            control_streams,
        }
    }

    /// Propagate current interests to all connected peers
    pub async fn propagate_interests(&self) -> Result<(), Box<dyn std::error::Error>> {
        let interests = self
            .interest_tracker
            .get_node_interests(&self.node_id)
            .await;

        if let Some(patterns) = interests {
            let patterns: Vec<_> = patterns.into_iter().collect();
            if !patterns.is_empty() {
                let pattern_count = patterns.len();
                let msg = InterestMessage::Set { patterns };
                let mut buf = Vec::new();
                ciborium::into_writer(&msg, &mut buf)?;

                // Send to all control streams
                let control_streams = self.control_streams.read().await;
                for (peer, stream) in control_streams.iter() {
                    if let Err(e) = stream.send(buf.clone()).await {
                        warn!("Failed to send interests to {}: {}", peer, e);
                    } else {
                        debug!("Sent {} interests to {}", pattern_count, peer);
                    }
                }
            }
        }

        Ok(())
    }

    /// Send interests to a specific stream
    pub async fn send_interests_to_stream(
        &self,
        stream: &Stream,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let interests = self
            .interest_tracker
            .get_node_interests(&self.node_id)
            .await;

        if let Some(patterns) = interests {
            let patterns: Vec<_> = patterns.into_iter().collect();
            if !patterns.is_empty() {
                let msg = InterestMessage::Set { patterns };
                let mut buf = Vec::new();
                ciborium::into_writer(&msg, &mut buf)?;
                stream.send(buf).await?;
            }
        }

        Ok(())
    }
}
