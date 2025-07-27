//! Handler for InitializeStreamInGroup command

use async_trait::async_trait;
use tracing::{debug, error, info};

use crate::consensus::group::types::{AdminOperation, GroupRequest};
use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::services::group_consensus::commands::InitializeStreamInGroup;
use crate::services::group_consensus::types::ConsensusLayers;
use proven_storage::StorageAdaptor;
use proven_topology::NodeId;

/// Handler for InitializeStreamInGroup command
pub struct InitializeStreamInGroupHandler<S>
where
    S: StorageAdaptor,
{
    /// Node ID
    node_id: NodeId,
    /// Map of group IDs to consensus layers
    groups: ConsensusLayers<S>,
}

impl<S> InitializeStreamInGroupHandler<S>
where
    S: StorageAdaptor,
{
    pub fn new(node_id: NodeId, groups: ConsensusLayers<S>) -> Self {
        Self { node_id, groups }
    }
}

#[async_trait]
impl<S> RequestHandler<InitializeStreamInGroup> for InitializeStreamInGroupHandler<S>
where
    S: StorageAdaptor + 'static,
{
    async fn handle(
        &self,
        request: InitializeStreamInGroup,
        _metadata: EventMetadata,
    ) -> Result<(), EventError> {
        info!(
            "Initializing stream {} in local group {:?}",
            request.stream_name, request.group_id
        );

        let groups_guard = self.groups.read().await;
        if let Some(group) = groups_guard.get(&request.group_id) {
            // Initialize the stream in group consensus
            let group_request = GroupRequest::Admin(AdminOperation::InitializeStream {
                stream: request.stream_name.clone(),
            });

            match group.submit_request(group_request).await {
                Ok(_) => {
                    debug!(
                        "Successfully initialized stream {} in group {:?}",
                        request.stream_name, request.group_id
                    );
                }
                Err(e) if e.is_not_leader() => {
                    // Not the leader - this is expected and fine
                    debug!(
                        "Not the leader for group {:?}, stream {} will be initialized by leader",
                        request.group_id, request.stream_name
                    );
                }
                Err(e) => {
                    error!(
                        "Failed to initialize stream {} in group {:?}: {}",
                        request.stream_name, request.group_id, e
                    );
                }
            }
        } else {
            info!(
                "Group {:?} not found locally for stream {} initialization",
                request.group_id, request.stream_name
            );
        }

        Ok(())
    }
}
