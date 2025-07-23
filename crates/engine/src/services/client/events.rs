//! Events emitted by the client service

use crate::foundation::types::ConsensusGroupId;
use crate::services::event::traits::ServiceEvent;
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

impl ServiceEvent for ClientServiceEvent {
    fn event_name(&self) -> &'static str {
        match self {
            Self::LearnedStreamExists { .. } => "ClientServiceEvent::LearnedStreamExists",
        }
    }
}
