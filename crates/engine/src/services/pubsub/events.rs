//! Event types for PubSub service integration

use bytes::Bytes;
use uuid::Uuid;

use super::types::PubSubNetworkMessage;
use crate::foundation::events::Event;
use crate::foundation::types::Subject;

/// Client-facing message type (without internal metadata)
#[derive(Debug, Clone)]
pub struct PubSubMessage {
    pub subject: Subject,
    pub payload: Bytes,
    pub headers: Vec<(String, String)>,
}

impl From<PubSubNetworkMessage> for PubSubMessage {
    fn from(msg: PubSubNetworkMessage) -> Self {
        Self {
            subject: msg.subject,
            payload: msg.payload,
            headers: msg.headers,
        }
    }
}

/// Response events from PubSub service (sent back to client)
#[derive(Debug, Clone)]
pub enum PubSubServiceEvent {
    /// Message received on a subscribed topic
    MessageReceived {
        /// The message that was received
        message: PubSubMessage,
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
