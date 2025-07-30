//! Commands for the PubSub service

use std::time::Duration;

use crate::foundation::Message;
use crate::foundation::events::{Request, StreamRequest};
use crate::foundation::types::{Subject, SubjectPattern};

/// Publish messages to a PubSub subject
#[derive(Debug, Clone)]
pub struct PublishMessage {
    pub subject: Subject,
    pub messages: Vec<Message>,
}

impl Request for PublishMessage {
    type Response = ();

    fn request_type() -> &'static str {
        "PublishMessage"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(5)
    }
}

/// Subscribe to a PubSub subject pattern with streaming
///
/// This returns a stream of messages directly from the event bus
/// without any intermediate channels.
#[derive(Debug, Clone)]
pub struct Subscribe {
    pub subject_pattern: SubjectPattern,
    pub queue_group: Option<String>,
}

impl StreamRequest for Subscribe {
    type Item = Message;

    fn request_type() -> &'static str {
        "PubSub.Subscribe"
    }

    fn default_timeout() -> Duration {
        // Subscriptions can be long-lived
        Duration::from_secs(86400) // 24 hours
    }
}

/// Handle for managing a streaming subscription
#[derive(Debug, Clone)]
pub struct StreamingSubscriptionHandle {
    pub id: String,
    /// Can be used to control the subscription (pause, resume, unsubscribe)
    pub control_tx: flume::Sender<SubscriptionControl>,
}

/// Control messages for streaming subscriptions
#[derive(Debug)]
pub enum SubscriptionControl {
    /// Pause message delivery
    Pause,
    /// Resume message delivery
    Resume,
    /// Unsubscribe and close the stream
    Unsubscribe,
}

/// Check if there are any responders for a subject
#[derive(Debug, Clone)]
pub struct HasResponders {
    pub subject: Subject,
}

impl Request for HasResponders {
    type Response = bool;

    fn request_type() -> &'static str {
        "PubSub.HasResponders"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(1)
    }
}
