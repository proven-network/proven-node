//! Streaming commands for the Stream service
//!
//! These commands use the StreamRequest pattern for efficient message delivery
//! without intermediate channels or session management.

use std::time::Duration;

use crate::foundation::events::StreamRequest;
use crate::services::stream::StoredMessage;
use proven_storage::LogIndex;

/// Stream messages from a stream
///
/// This returns a stream of messages directly from the stream service
/// without any intermediate channels or session management.
#[derive(Debug, Clone)]
pub struct StreamMessages {
    pub stream_name: String,
    pub start_sequence: LogIndex,
    pub end_sequence: Option<LogIndex>,
}

impl StreamRequest for StreamMessages {
    type Item = StoredMessage;

    fn request_type() -> &'static str {
        "Stream.StreamMessages"
    }

    fn default_timeout() -> Duration {
        Duration::from_secs(300) // 5 minutes for long streams
    }
}
