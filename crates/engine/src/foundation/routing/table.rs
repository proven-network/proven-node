//! Main routing table implementation

use crate::foundation::models::stream::StreamPlacement;
use crate::foundation::{ConsensusGroupId, GlobalStateRead, GlobalStateReader};
use proven_topology::NodeId;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::types::{GroupLocation, GroupRoute, RoutingDecision, RoutingError, StreamRoute};

/// Thread-safe routing table that queries GlobalStateReader for all routing decisions
pub struct RoutingTable {
    /// Local node ID for determining local vs remote
    local_node_id: NodeId,

    /// Global state reader (late-bound)
    global_state: Arc<RwLock<Option<GlobalStateReader>>>,
}

impl RoutingTable {
    /// Create a new routing table
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            global_state: Arc::new(RwLock::new(None)),
        }
    }

    /// Set the global state reader (called when GlobalConsensusService starts)
    pub async fn set_global_state(&self, global_state: GlobalStateReader) {
        *self.global_state.write().await = Some(global_state);
    }

    /// Get the global state reader or return error if not initialized
    async fn require_global_state(&self) -> Result<GlobalStateReader, RoutingError> {
        self.global_state
            .read()
            .await
            .as_ref()
            .cloned()
            .ok_or(RoutingError::NotInitialized)
    }

    /// Get a stream route
    pub async fn get_stream_route(
        &self,
        stream_name: &str,
    ) -> Result<Option<StreamRoute>, RoutingError> {
        let global_state = self.require_global_state().await?;
        let stream_name_obj = crate::foundation::StreamName::from(stream_name);

        Ok(global_state
            .get_stream(&stream_name_obj)
            .await
            .map(|stream_info| StreamRoute {
                stream_name: stream_name.to_string(),
                placement: stream_info.placement,
                assigned_at: std::time::UNIX_EPOCH
                    + std::time::Duration::from_secs(stream_info.created_at),
                is_active: true,
            }))
    }

    /// Get a group route
    pub async fn get_group_route(
        &self,
        group_id: ConsensusGroupId,
    ) -> Result<Option<GroupRoute>, RoutingError> {
        let global_state = self.require_global_state().await?;

        if let Some(group_info) = global_state.get_group(&group_id).await {
            // Count streams in this group
            let all_streams = global_state.get_all_streams().await;
            let stream_count = all_streams
                .iter()
                .filter(|s| matches!(s.placement, StreamPlacement::Group(id) if id == group_id))
                .count();

            Ok(Some(GroupRoute {
                group_id,
                members: group_info.members,
                stream_count,
            }))
        } else {
            Ok(None)
        }
    }

    // Query methods

    /// Check if a group is local to this node
    pub async fn is_group_local(&self, group_id: ConsensusGroupId) -> Result<bool, RoutingError> {
        let global_state = self.require_global_state().await?;

        global_state
            .get_group(&group_id)
            .await
            .map(|group_info| group_info.members.contains(&self.local_node_id))
            .ok_or(RoutingError::GroupNotFound(group_id))
    }

    /// Get the location of a group relative to this node
    pub async fn get_group_location(
        &self,
        group_id: ConsensusGroupId,
    ) -> Result<GroupLocation, RoutingError> {
        let global_state = self.require_global_state().await?;

        global_state
            .get_group(&group_id)
            .await
            .map(|group_info| {
                if group_info.members.contains(&self.local_node_id) {
                    GroupLocation::Local
                } else {
                    GroupLocation::Remote
                }
            })
            .ok_or(RoutingError::GroupNotFound(group_id))
    }

    /// Find the least loaded group (local groups preferred)
    pub async fn find_least_loaded_group(&self) -> Option<ConsensusGroupId> {
        let global_state = self.require_global_state().await.ok()?;

        let all_groups = global_state.get_all_groups().await;
        let all_streams = global_state.get_all_streams().await;

        let mut group_stream_counts = Vec::new();

        // Count streams for each group
        for group in all_groups {
            let stream_count = all_streams
                .iter()
                .filter(|s| matches!(s.placement, StreamPlacement::Group(id) if id == group.id))
                .count();
            let is_local = group.members.contains(&self.local_node_id);
            group_stream_counts.push((group.id, stream_count, is_local));
        }

        // Sort by: local first, then by stream count
        group_stream_counts.sort_by(|a, b| match (a.2, b.2) {
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            _ => a.1.cmp(&b.1),
        });

        group_stream_counts.first().map(|(id, _, _)| *id)
    }

    /// Get all streams assigned to a group
    pub async fn get_streams_by_group(&self, group_id: ConsensusGroupId) -> Vec<String> {
        let global_state = match self.require_global_state().await {
            Ok(state) => state,
            Err(_) => return Vec::new(),
        };

        global_state
            .get_all_streams()
            .await
            .into_iter()
            .filter(
                |stream| matches!(stream.placement, StreamPlacement::Group(id) if id == group_id),
            )
            .map(|stream| stream.stream_name.to_string())
            .collect()
    }

    /// Get routing decision for a stream
    pub async fn get_stream_routing(
        &self,
        stream_name: &str,
    ) -> Result<RoutingDecision, RoutingError> {
        let global_state = self.require_global_state().await?;
        let stream_name_obj = crate::foundation::StreamName::from(stream_name);

        if let Some(stream_info) = global_state.get_stream(&stream_name_obj).await {
            match stream_info.placement {
                StreamPlacement::Global => Ok(RoutingDecision::Global),
                StreamPlacement::Group(group_id) => {
                    // Check if the group is local
                    match self.get_group_location(group_id).await? {
                        GroupLocation::Local => Ok(RoutingDecision::Local),
                        GroupLocation::Remote => Ok(RoutingDecision::Group(group_id)),
                    }
                }
            }
        } else {
            Err(RoutingError::StreamNotFound(stream_name.to_string()))
        }
    }

    /// Get routing decision for a stream
    pub async fn get_routing_decision(
        &self,
        stream_name: &str,
    ) -> Result<RoutingDecision, RoutingError> {
        // Use the new get_stream_routing method which handles both global and group streams
        match self.get_stream_routing(stream_name).await {
            Ok(decision) => Ok(decision),
            Err(RoutingError::StreamNotFound(_)) => {
                // Stream doesn't exist yet, find best group for new stream
                if let Some(group_id) = self.find_least_loaded_group().await {
                    Ok(RoutingDecision::Group(group_id))
                } else {
                    Ok(RoutingDecision::Reject {
                        reason: "No available groups".to_string(),
                    })
                }
            }
            Err(e) => Err(e),
        }
    }
}
