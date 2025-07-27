//! Event types for PubSub service integration

use uuid::Uuid;

use super::types::PubSubNetworkMessage;
use crate::foundation::events::Event;

use crate::foundation::Message;

impl From<PubSubNetworkMessage> for Message {
    fn from(msg: PubSubNetworkMessage) -> Self {
        let mut message = Message::new(msg.payload);

        // Add all headers (subject is already in headers)
        for (key, value) in msg.headers {
            message = message.with_header(key, value);
        }

        message
    }
}

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
