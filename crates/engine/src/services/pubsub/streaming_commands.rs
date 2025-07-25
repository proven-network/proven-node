//! Streaming commands for the PubSub service
//!
//! These commands use the StreamRequest pattern for efficient message delivery
//! without intermediate channels.

use std::time::Duration;

use crate::foundation::events::{Error, StreamRequest};
use crate::foundation::types::SubjectPattern;
use crate::services::pubsub::PubSubMessage;

/// Subscribe to a PubSub subject pattern with streaming
///
/// This returns a stream of messages directly from the event bus
/// without any intermediate channels.
#[derive(Debug, Clone)]
pub struct SubscribeStream {
    pub subject_pattern: SubjectPattern,
    pub queue_group: Option<String>,
}

impl StreamRequest for SubscribeStream {
    type Item = PubSubMessage;

    fn request_type() -> &'static str {
        "PubSub.SubscribeStream"
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
