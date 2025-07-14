//! Event system for cross-layer communication
//!
//! This module provides an event-driven communication system that allows
//! different layers to communicate asynchronously while maintaining clear boundaries.

use crate::core::global::StreamConfig;
use crate::core::group::MigrationState;
use crate::{ConsensusGroupId, NodeId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, oneshot};
use uuid::Uuid;

/// Events that can be emitted by consensus layers
#[derive(Debug, Clone)]
pub enum ConsensusEvent {
    /// A stream was created in global consensus
    StreamCreated {
        /// Stream name
        name: String,
        /// Stream configuration
        config: StreamConfig,
        /// Target consensus group
        group_id: ConsensusGroupId,
        /// Channel for reply
        reply_to: EventReplyChannel,
    },

    /// A stream was deleted from global consensus
    StreamDeleted {
        /// Stream name
        name: String,
        /// Consensus group it was in
        group_id: ConsensusGroupId,
        /// Channel for reply
        reply_to: EventReplyChannel,
    },

    /// Stream configuration was updated
    StreamConfigUpdated {
        /// Stream name
        name: String,
        /// New configuration
        config: StreamConfig,
        /// Consensus group
        group_id: ConsensusGroupId,
        /// Channel for reply
        reply_to: EventReplyChannel,
    },

    /// Stream is migrating between groups
    StreamMigrating {
        /// Stream name
        name: String,
        /// Source group
        from_group: ConsensusGroupId,
        /// Target group
        to_group: ConsensusGroupId,
        /// Migration state
        state: MigrationState,
        /// Channel for reply
        reply_to: EventReplyChannel,
    },

    /// Stream was reallocated to a new group
    StreamReallocated {
        /// Stream name
        name: String,
        /// Previous group (if any)
        old_group: Option<ConsensusGroupId>,
        /// New group
        new_group: ConsensusGroupId,
        /// Channel for reply
        reply_to: EventReplyChannel,
    },

    /// A consensus group was created
    GroupCreated {
        /// Group ID
        group_id: ConsensusGroupId,
        /// Initial members
        members: Vec<NodeId>,
        /// Channel for reply
        reply_to: EventReplyChannel,
    },

    /// A consensus group was deleted
    GroupDeleted {
        /// Group ID
        group_id: ConsensusGroupId,
        /// Channel for reply
        reply_to: EventReplyChannel,
    },

    /// Stream migration started
    StreamMigrationStarted {
        /// Stream name
        stream_name: String,
        /// Source group
        from_group: ConsensusGroupId,
        /// Target group
        to_group: ConsensusGroupId,
        /// Channel for reply
        reply_to: EventReplyChannel,
    },

    /// Node added to group
    NodeAddedToGroup {
        /// Node ID
        node_id: NodeId,
        /// Group ID
        group_id: ConsensusGroupId,
        /// Channel for reply
        reply_to: EventReplyChannel,
    },

    /// Node removed from group
    NodeRemovedFromGroup {
        /// Node ID
        node_id: NodeId,
        /// Group ID
        group_id: ConsensusGroupId,
        /// Channel for reply
        reply_to: EventReplyChannel,
    },
}

/// Result of an event handler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventResult {
    /// Operation succeeded
    Success,
    /// Operation failed with error
    Failed(String),
    /// Operation is pending/async
    Pending(Uuid),
}

/// Channel for replying to events
#[derive(Debug, Clone)]
pub struct EventReplyChannel {
    inner: Arc<oneshot::Sender<EventResult>>,
}

impl EventReplyChannel {
    /// Create a new reply channel
    pub fn new(sender: oneshot::Sender<EventResult>) -> Self {
        Self {
            inner: Arc::new(sender),
        }
    }

    /// Send a reply
    pub fn reply(self, result: EventResult) -> Result<(), EventResult> {
        Arc::try_unwrap(self.inner)
            .map_err(|_| result.clone())
            .and_then(|sender| sender.send(result))
    }
}

/// Event bus for publishing and subscribing to consensus events
pub struct EventBus {
    /// Broadcast channel for events
    sender: broadcast::Sender<ConsensusEvent>,
    /// Subscription tracking
    subscribers: Arc<RwLock<Vec<String>>>,
}

impl EventBus {
    /// Create a new event bus
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            sender,
            subscribers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Publish an event
    pub async fn publish(&self, event: ConsensusEvent) -> Result<(), String> {
        self.sender
            .send(event)
            .map(|_| ())
            .map_err(|e| format!("Failed to publish event: {e}"))
    }

    /// Subscribe to events
    pub async fn subscribe(&self, subscriber_id: String) -> EventSubscriber {
        let receiver = self.sender.subscribe();
        self.subscribers.write().await.push(subscriber_id.clone());

        EventSubscriber {
            id: subscriber_id,
            receiver,
        }
    }

    /// Get number of active subscribers
    pub async fn subscriber_count(&self) -> usize {
        self.subscribers.read().await.len()
    }
}

/// Event subscriber handle
pub struct EventSubscriber {
    /// Subscriber ID
    pub id: String,
    /// Receiver for events
    receiver: broadcast::Receiver<ConsensusEvent>,
}

impl EventSubscriber {
    /// Receive the next event
    pub async fn recv(&mut self) -> Option<ConsensusEvent> {
        self.receiver.recv().await.ok()
    }

    /// Try to receive without blocking
    pub fn try_recv(&mut self) -> Option<ConsensusEvent> {
        self.receiver.try_recv().ok()
    }
}

/// Helper to create a reply channel pair
pub fn create_reply_channel() -> (EventReplyChannel, oneshot::Receiver<EventResult>) {
    let (tx, rx) = oneshot::channel();
    (EventReplyChannel::new(tx), rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_event_bus_pub_sub() {
        let bus = EventBus::new(100);

        // Create subscriber
        let mut sub = bus.subscribe("test".to_string()).await;

        // Publish event
        let (reply_tx, _reply_rx) = create_reply_channel();
        let event = ConsensusEvent::StreamCreated {
            name: "test-stream".to_string(),
            config: StreamConfig::default(),
            group_id: ConsensusGroupId::new(1),
            reply_to: reply_tx,
        };

        bus.publish(event.clone()).await.unwrap();

        // Receive event
        let received = sub.recv().await.unwrap();
        match received {
            ConsensusEvent::StreamCreated { name, .. } => {
                assert_eq!(name, "test-stream");
            }
            _ => panic!("Wrong event type"),
        }
    }
}
