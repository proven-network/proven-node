//! Commands for the stream service (request-response patterns)

use bytes::Bytes;
use proven_storage::LogIndex;
use std::sync::Arc;
use std::time::Duration;

use crate::foundation::events::{Request, StreamRequest};
use crate::foundation::types::{ConsensusGroupId, StreamName};
use crate::foundation::{Message, StreamConfig, StreamInfo};

/// Persist messages to a stream (from group consensus)
#[derive(Debug, Clone)]
pub struct PersistMessages {
    pub stream_name: StreamName,
    /// Pre-serialized entries ready for storage
    pub entries: Arc<Vec<Bytes>>,
}

impl Request for PersistMessages {
    type Response = ();

    fn request_type() -> &'static str {
        "PersistMessages"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(30)
    }
}

/// Create a new stream
#[derive(Debug, Clone)]
pub struct CreateStream {
    pub name: StreamName,
    pub config: StreamConfig,
    pub group_id: ConsensusGroupId,
}

impl Request for CreateStream {
    type Response = ();

    fn request_type() -> &'static str {
        "CreateStream"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(10)
    }
}

/// Delete a stream
#[derive(Debug, Clone)]
pub struct DeleteStream {
    pub name: StreamName,
}

impl Request for DeleteStream {
    type Response = ();

    fn request_type() -> &'static str {
        "DeleteStream"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(10)
    }
}

/// Stream messages from a stream
///
/// This returns a stream of messages directly from the stream service
/// without any intermediate channels or session management.
#[derive(Debug, Clone)]
pub struct StreamMessages {
    pub stream_name: String,
    pub start_sequence: Option<LogIndex>, // None = start from beginning
}

impl StreamRequest for StreamMessages {
    type Item = (Message, u64, u64); // (message, timestamp, sequence)

    fn request_type() -> &'static str {
        "Stream.StreamMessages"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(300) // 5 minutes for long streams
    }
}

/// Register a stream that was created through consensus
/// This is used when other nodes learn about a stream through consensus callbacks
#[derive(Debug, Clone)]
pub struct RegisterStream {
    pub stream_name: StreamName,
    pub config: StreamConfig,
    pub placement: crate::foundation::models::stream::StreamPlacement,
}

impl Request for RegisterStream {
    type Response = ();

    fn request_type() -> &'static str {
        "RegisterStream"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(5)
    }
}
