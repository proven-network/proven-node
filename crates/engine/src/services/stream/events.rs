//! Events emitted by the stream service (new event system)

use crate::foundation::events::Event;
use crate::foundation::types::ConsensusGroupId;
use crate::services::stream::{StreamConfig, StreamName};

/// Events emitted by the stream service
#[derive(Debug, Clone)]
pub enum StreamEvent {
    /// Stream was created
    StreamCreated {
        name: StreamName,
        config: StreamConfig,
        group_id: ConsensusGroupId,
    },

    /// Stream was deleted
    StreamDeleted { name: StreamName },

    /// Stream configuration changed
    StreamConfigChanged {
        name: StreamName,
        old_config: StreamConfig,
        new_config: StreamConfig,
    },

    /// Stream data was appended
    StreamAppended {
        name: StreamName,
        offset: u64,
        count: usize,
    },

    /// Stream was truncated
    StreamTruncated {
        name: StreamName,
        new_start_offset: u64,
    },

    /// Stream consumer subscribed
    ConsumerSubscribed {
        stream_name: StreamName,
        consumer_id: String,
        start_offset: u64,
    },

    /// Stream consumer unsubscribed
    ConsumerUnsubscribed {
        stream_name: StreamName,
        consumer_id: String,
    },
}

impl Event for StreamEvent {
    fn event_type() -> &'static str {
        "StreamEvent"
    }
}
