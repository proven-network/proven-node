//! Events emitted by the client service

use bytes::Bytes;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::foundation::types::{ConsensusGroupId, Subject, SubjectPattern};
use crate::services::event::traits::ServiceEvent;
use crate::services::pubsub::PubSubMessage;
use crate::services::stream::StreamName;

/// Events emitted by the client service
#[derive(Debug, Clone)]
pub enum ClientServiceEvent {
    /// Learned about an existing stream (e.g., from StreamAlreadyExists response)
    LearnedStreamExists {
        stream_name: StreamName,
        group_id: ConsensusGroupId,
    },

    /// Request to publish a message via PubSub
    PubSubPublish {
        request_id: Uuid,
        subject: Subject,
        payload: Bytes,
        headers: Vec<(String, String)>,
    },

    /// Request to subscribe to a PubSub subject pattern
    PubSubSubscribe {
        request_id: Uuid,
        subject_pattern: SubjectPattern,
        queue_group: Option<String>,
        /// Channel to receive messages on this subscription
        message_tx: broadcast::Sender<PubSubMessage>,
    },

    /// Request to unsubscribe from a PubSub subscription
    PubSubUnsubscribe {
        request_id: Uuid,
        subscription_id: String,
    },
}

impl ServiceEvent for ClientServiceEvent {
    fn event_name(&self) -> &'static str {
        match self {
            Self::LearnedStreamExists { .. } => "ClientServiceEvent::LearnedStreamExists",
            Self::PubSubPublish { .. } => "ClientServiceEvent::PubSubPublish",
            Self::PubSubSubscribe { .. } => "ClientServiceEvent::PubSubSubscribe",
            Self::PubSubUnsubscribe { .. } => "ClientServiceEvent::PubSubUnsubscribe",
        }
    }
}
