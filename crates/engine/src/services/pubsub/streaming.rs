//! Streaming implementation for PubSub service
//!
//! This module implements the streaming protocol for efficient PubSub message delivery.
//! It replaces the request-response pattern with persistent streams between nodes.

use async_trait::async_trait;
use bytes::Bytes;
use proven_network::{NetworkResult, Stream, StreamingService};
use proven_topology::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::foundation::Message;
use crate::foundation::types::SubjectPattern;
use crate::services::pubsub::internal::InterestManager;

/// Interest update messages sent via control streams
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InterestMessage {
    /// Replace all interests with this set
    Set { patterns: Vec<SubjectPattern> },
    /// Add new interests
    Add { patterns: Vec<SubjectPattern> },
    /// Remove specific interests
    Remove { patterns: Vec<SubjectPattern> },
}

/// PubSub streaming service for handling persistent message streams
pub struct PubSubStreamingService {
    interest_manager: Arc<dyn InterestManager>,
}

impl PubSubStreamingService {
    pub fn new(interest_manager: Arc<dyn InterestManager>) -> Self {
        Self { interest_manager }
    }
}

#[async_trait]
impl StreamingService for PubSubStreamingService {
    fn stream_type(&self) -> &'static str {
        "pubsub"
    }

    async fn handle_stream(
        &self,
        peer: NodeId,
        stream: Stream,
        metadata: HashMap<String, String>,
    ) -> NetworkResult<()> {
        info!(
            "Handling PubSub stream from {} with metadata: {:?}",
            peer, metadata
        );

        // Determine stream purpose from metadata
        match metadata.get("channel").map(|s| s.as_str()) {
            Some("publish") => {
                // This is a publish stream - we'll receive messages to route
                self.handle_publish_stream(peer, stream).await
            }
            Some("control") => {
                // This is a control stream - we'll receive interest updates
                self.handle_control_stream(peer, stream).await
            }
            _ => {
                warn!("Unknown stream channel from {}", peer);
                Err(proven_network::NetworkError::InvalidMessage(
                    "Unknown stream channel".to_string(),
                ))
            }
        }
    }
}

impl PubSubStreamingService {
    /// Handle incoming publish stream from a peer
    async fn handle_publish_stream(&self, peer: NodeId, stream: Stream) -> NetworkResult<()> {
        debug!("Handling publish stream from {}", peer);

        while let Some(data) = stream.recv().await {
            // Deserialize the message
            match ciborium::from_reader::<Message, _>(data.as_ref()) {
                Ok(mut message) => {
                    // Ensure the message has a source header
                    if message.get_header("source").is_none() {
                        message = message.with_header("source", peer.to_string());
                    }

                    // Route the message to local subscribers
                    let count = self.interest_manager.route_message(&message).await;
                    if count > 0 {
                        debug!(
                            "Routed message from {} to {} local subscribers",
                            peer, count
                        );
                    }
                }
                Err(e) => {
                    error!("Failed to deserialize message from {}: {}", peer, e);
                }
            }
        }

        debug!("Publish stream from {} closed", peer);
        Ok(())
    }

    /// Handle incoming control stream from a peer
    async fn handle_control_stream(&self, peer: NodeId, stream: Stream) -> NetworkResult<()> {
        debug!("Handling control stream from {}", peer);

        while let Some(data) = stream.recv().await {
            // Deserialize control message
            match ciborium::from_reader::<InterestMessage, _>(data.as_ref()) {
                Ok(msg) => match msg {
                    InterestMessage::Set { patterns } => {
                        debug!("Setting {} interest patterns from {}", patterns.len(), peer);
                        self.interest_manager
                            .update_peer_interests(peer.clone(), patterns)
                            .await;
                    }
                    InterestMessage::Add { patterns } => {
                        debug!("Adding {} interest patterns from {}", patterns.len(), peer);
                        for pattern in patterns {
                            self.interest_manager
                                .add_node_interest(peer.clone(), pattern)
                                .await;
                        }
                    }
                    InterestMessage::Remove { patterns } => {
                        debug!(
                            "Removing {} interest patterns from {}",
                            patterns.len(),
                            peer
                        );
                        for pattern in patterns {
                            self.interest_manager
                                .remove_node_interest(peer.clone(), &pattern)
                                .await;
                        }
                    }
                },
                Err(e) => {
                    error!("Failed to deserialize control message from {}: {}", peer, e);
                }
            }
        }

        // Stream closed - remove all interests for this peer
        debug!(
            "Control stream from {} closed, removing all interests",
            peer
        );
        self.interest_manager.remove_all_node_interests(&peer).await;

        Ok(())
    }
}
