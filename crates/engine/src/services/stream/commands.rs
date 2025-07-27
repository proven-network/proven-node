//! Commands for the stream service (request-response patterns)

use bytes::Bytes;
use proven_storage::LogIndex;
use std::sync::Arc;
use std::time::Duration;

use crate::foundation::events::Request;
use crate::foundation::types::{ConsensusGroupId, StreamName};
use crate::foundation::{StoredMessage, StreamConfig};

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

/// Read messages from a stream
#[derive(Debug, Clone)]
pub struct ReadMessages {
    pub stream_name: StreamName,
    pub start_offset: LogIndex,
    pub count: u64,
}

impl Request for ReadMessages {
    type Response = Vec<StoredMessage>;

    fn request_type() -> &'static str {
        "ReadMessages"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(10)
    }
}

/// Get stream information
#[derive(Debug, Clone)]
pub struct GetStreamInfo {
    pub stream_name: StreamName,
}

impl Request for GetStreamInfo {
    type Response = Option<StreamInfo>;

    fn request_type() -> &'static str {
        "GetStreamInfo"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(5)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StreamInfo {
    pub name: StreamName,
    pub config: StreamConfig,
    pub group_id: ConsensusGroupId,
    pub start_offset: u64,
    pub end_offset: u64,
}
