//! Commands for the PubSub service (request-response patterns)

use bytes::Bytes;
use std::time::Duration;
use tokio::sync::broadcast;

use crate::foundation::events::{Error, Request};
use crate::foundation::types::{Subject, SubjectPattern};
use crate::services::pubsub::PubSubMessage;

/// Publish a message to a PubSub subject
#[derive(Debug, Clone)]
pub struct PublishMessage {
    pub subject: Subject,
    pub payload: Bytes,
    pub headers: Vec<(String, String)>,
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

/// Subscribe to a PubSub subject pattern
#[derive(Debug, Clone)]
pub struct Subscribe {
    pub subject_pattern: SubjectPattern,
    pub queue_group: Option<String>,
    /// Channel to receive messages on this subscription
    pub message_tx: broadcast::Sender<PubSubMessage>,
}

impl Request for Subscribe {
    type Response = String; // Returns subscription ID

    fn request_type() -> &'static str {
        "Subscribe"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(5)
    }
}

/// Unsubscribe from a PubSub subscription
#[derive(Debug, Clone)]
pub struct Unsubscribe {
    pub subscription_id: String,
}

impl Request for Unsubscribe {
    type Response = ();

    fn request_type() -> &'static str {
        "Unsubscribe"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(5)
    }
}
