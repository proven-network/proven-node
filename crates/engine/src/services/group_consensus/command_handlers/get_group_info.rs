//! Handler for GetGroupInfo command

use async_trait::async_trait;

use crate::foundation::GroupStateRead;
use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::services::group_consensus::commands::{GetGroupInfo, GroupInfo};
use crate::services::group_consensus::types::{ConsensusLayers, States};
use proven_storage::StorageAdaptor;

/// Handler for GetGroupInfo command
pub struct GetGroupInfoHandler<S>
where
    S: StorageAdaptor,
{
    /// Map of group IDs to consensus layers
    groups: ConsensusLayers<S>,
    /// Map of group IDs to state readers
    group_states: States,
}

impl<S> GetGroupInfoHandler<S>
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
impl<S> RequestHandler<GetGroupInfo> for GetGroupInfoHandler<S>
where
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: GetGroupInfo,
        _metadata: EventMetadata,
    ) -> Result<Option<GroupInfo>, EventError> {
        let groups_guard = self.groups.read().await;
        if let Some(group) = groups_guard.get(&request.group_id) {
            let leader = group.current_leader().await;
            let members = group.current_members().await;

            // Get streams from group state
            let states_guard = self.group_states.read().await;
            let streams = if let Some(state) = states_guard.get(&request.group_id) {
                state.list_streams().await
            } else {
                vec![]
            };
            drop(states_guard);

            Ok(Some(GroupInfo {
                group_id: request.group_id,
                members,
                leader,
                streams,
            }))
        } else {
            Ok(None)
        }
    }
}
