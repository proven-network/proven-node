//! Events emitted by the client service (new event system)

use crate::foundation::events::Event;
use crate::foundation::types::ConsensusGroupId;
use crate::services::stream::StreamName;

/// Events emitted by the client service
#[derive(Debug, Clone)]
pub enum ClientServiceEvent {
    /// Learned about an existing stream (e.g., from StreamAlreadyExists response)
    LearnedStreamExists {
        stream_name: StreamName,
        group_id: ConsensusGroupId,
    },
}

impl Event for ClientServiceEvent {
    fn event_type() -> &'static str {
        "ClientServiceEvent"
    }
}
