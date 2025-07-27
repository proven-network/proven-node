//! Handler for GetStreamState command

use async_trait::async_trait;

use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::foundation::models::stream::StreamState;
use crate::foundation::{GroupStateRead, StreamName};
use crate::services::group_consensus::commands::GetStreamState;
use crate::services::group_consensus::types::{ConsensusLayers, States};
use proven_storage::StorageAdaptor;

/// Handler for GetStreamState command
pub struct GetStreamStateHandler<S>
where
    S: StorageAdaptor,
{
    /// Map of group IDs to consensus layers
    groups: ConsensusLayers<S>,
    /// Map of group IDs to state readers
    group_states: States,
}

impl<S> GetStreamStateHandler<S>
where
    S: StorageAdaptor,
{
    pub fn new(groups: ConsensusLayers<S>, group_states: States) -> Self {
        Self {
            groups,
            group_states,
        }
    }
}

#[async_trait]
impl<S> RequestHandler<GetStreamState> for GetStreamStateHandler<S>
where
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: GetStreamState,
        _metadata: EventMetadata,
    ) -> Result<Option<StreamState>, EventError> {
        let states_guard = self.group_states.read().await;
        if let Some(state) = states_guard.get(&request.group_id) {
            let stream_name = StreamName::new(request.stream_name.as_str());
            Ok(state.get_stream(&stream_name).await)
        } else {
            Ok(None)
        }
    }
}
