//! Event types for PubSub service integration

use uuid::Uuid;

use crate::foundation::Message;
use crate::foundation::events::Event;

/// Response events from PubSub service (sent back to client)
#[derive(Debug, Clone)]
pub enum PubSubServiceEvent {
    /// Message received on a subscribed topic
    MessageReceived {
        /// The message that was received
        message: Message,
        /// Subject pattern that matched (useful for wildcard subscriptions)
        matched_pattern: String,
    },
    /// Publish completed
    PublishComplete { request_id: Uuid },

    /// Publish failed
    PublishError { request_id: Uuid, error: String },

    /// Subscribe completed
    SubscribeComplete {
        request_id: Uuid,
        subscription_id: String,
    },

    /// Subscribe failed
    SubscribeError { request_id: Uuid, error: String },

    /// Unsubscribe completed
    UnsubscribeComplete { request_id: Uuid },

    /// Unsubscribe failed
    UnsubscribeError { request_id: Uuid, error: String },
}

impl Event for PubSubServiceEvent {
    fn event_type() -> &'static str {
        "PubSubServiceEvent"
    }
}
