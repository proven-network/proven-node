//! Handler for node operations

use async_trait::async_trait;
use tracing::{debug, info};

use crate::{
    ConsensusGroupId, NodeId, core::global::GlobalState, error::ConsensusResult,
    operations::NodeOperation,
};

use super::{GlobalOperationHandler, NodeOperationResponse, OperationContext};

/// Handler for node operations
pub struct NodeOperationHandler {
    /// Maximum number of groups a node can be assigned to
    max_groups_per_node: usize,
}

impl NodeOperationHandler {
    /// Create a new node operation handler
    pub fn new(max_groups_per_node: usize) -> Self {
        Self {
            max_groups_per_node,
        }
    }

    /// Handle assigning a node to a group
    async fn handle_assign_to_group(
        &self,
        node_id: &NodeId,
        group_id: ConsensusGroupId,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<NodeOperationResponse> {
        let mut groups = state.consensus_groups.write().await;

        // Check if group exists
        let group_info = match groups.get_mut(&group_id) {
            Some(info) => info,
            None => {
                return Ok(NodeOperationResponse::Failed {
                    operation: "assign_to_group".to_string(),
                    reason: format!("Consensus group {group_id:?} not found"),
                });
            }
        };

        // Check if node is already in the group
        if group_info.members.contains(node_id) {
            return Ok(NodeOperationResponse::Failed {
                operation: "assign_to_group".to_string(),
                reason: format!("Node {node_id} is already a member of group {group_id:?}"),
            });
        }

        // Count how many groups this node is already in before adding
        // We need to drop the mutable reference first
        let group_id_to_add = group_id;
        let node_group_count = {
            let mut count = 0;
            // Iterate through all groups to count how many contain this node
            for (gid, g) in groups.iter() {
                if gid != &group_id_to_add && g.members.contains(node_id) {
                    count += 1;
                }
            }
            count
        };

        // Get the group again after counting
        let group_info = groups.get_mut(&group_id).unwrap();

        // Check if adding to this group would exceed the limit
        if node_group_count >= self.max_groups_per_node {
            return Ok(NodeOperationResponse::Failed {
                operation: "assign_to_group".to_string(),
                reason: format!(
                    "Node {} is already in {} groups (max: {})",
                    node_id, node_group_count, self.max_groups_per_node
                ),
            });
        }

        // Add node to group
        group_info.members.push(node_id.clone());

        info!(
            "Assigned node {} to consensus group {:?}",
            node_id, group_id
        );

        Ok(NodeOperationResponse::AssignedToGroup {
            sequence: context.sequence,
            node_id: node_id.clone(),
            group_id,
            total_groups: node_group_count + 1,
        })
    }

    /// Handle removing a node from a group
    async fn handle_remove_from_group(
        &self,
        node_id: &NodeId,
        group_id: ConsensusGroupId,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<NodeOperationResponse> {
        let mut groups = state.consensus_groups.write().await;

        // Check if group exists
        let group_info = match groups.get_mut(&group_id) {
            Some(info) => info,
            None => {
                return Ok(NodeOperationResponse::Failed {
                    operation: "remove_from_group".to_string(),
                    reason: format!("Consensus group {group_id:?} not found"),
                });
            }
        };

        // Check if node is in the group
        let member_index = match group_info.members.iter().position(|n| n == node_id) {
            Some(idx) => idx,
            None => {
                return Ok(NodeOperationResponse::Failed {
                    operation: "remove_from_group".to_string(),
                    reason: format!("Node {node_id} is not a member of group {group_id:?}"),
                });
            }
        };

        // Check if this would leave the group empty
        if group_info.members.len() == 1 {
            return Ok(NodeOperationResponse::Failed {
                operation: "remove_from_group".to_string(),
                reason: format!("Cannot remove last member from group {group_id:?}"),
            });
        }

        // Remove node from group
        group_info.members.remove(member_index);

        // Count remaining groups for this node
        let remaining_groups = groups
            .values()
            .filter(|g| g.members.contains(node_id))
            .count();

        info!(
            "Removed node {} from consensus group {:?}",
            node_id, group_id
        );

        Ok(NodeOperationResponse::RemovedFromGroup {
            sequence: context.sequence,
            node_id: node_id.clone(),
            group_id,
            remaining_groups,
        })
    }

    /// Handle updating all group assignments for a node
    async fn handle_update_groups(
        &self,
        node_id: &NodeId,
        new_group_ids: &[ConsensusGroupId],
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<NodeOperationResponse> {
        let mut groups = state.consensus_groups.write().await;

        // Validate all target groups exist
        for group_id in new_group_ids {
            if !groups.contains_key(group_id) {
                return Ok(NodeOperationResponse::Failed {
                    operation: "update_groups".to_string(),
                    reason: format!("Group {group_id:?} not found"),
                });
            }
        }

        // Check group limit
        if new_group_ids.len() > self.max_groups_per_node {
            return Ok(NodeOperationResponse::Failed {
                operation: "update_groups".to_string(),
                reason: format!(
                    "Too many groups: {} requested, max {}",
                    new_group_ids.len(),
                    self.max_groups_per_node
                ),
            });
        }

        // First, remove node from all current groups
        for group in groups.values_mut() {
            group.members.retain(|n| n != node_id);
        }

        // Then add to new groups
        for group_id in new_group_ids {
            if let Some(group) = groups.get_mut(group_id)
                && !group.members.contains(node_id)
            {
                group.members.push(node_id.clone());
            }
        }

        info!("Updated node {} groups to {:?}", node_id, new_group_ids);

        Ok(NodeOperationResponse::AssignedToGroup {
            sequence: context.sequence,
            node_id: node_id.clone(),
            group_id: new_group_ids
                .first()
                .copied()
                .unwrap_or(ConsensusGroupId::new(0)),
            total_groups: new_group_ids.len(),
        })
    }

    /// Handle decommissioning a node
    async fn handle_decommission(
        &self,
        node_id: &NodeId,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<NodeOperationResponse> {
        let mut groups = state.consensus_groups.write().await;

        // Remove node from all groups
        let mut removed_from = 0;
        for group in groups.values_mut() {
            if group.members.contains(node_id) {
                // Check if this would leave the group empty
                if group.members.len() == 1 {
                    return Ok(NodeOperationResponse::Failed {
                        operation: "decommission".to_string(),
                        reason: format!(
                            "Cannot decommission node {}: would leave group {:?} empty",
                            node_id, group.id
                        ),
                    });
                }
                group.members.retain(|n| n != node_id);
                removed_from += 1;
            }
        }

        info!(
            "Decommissioned node {}, removed from {} groups",
            node_id, removed_from
        );

        Ok(NodeOperationResponse::RemovedFromGroup {
            sequence: context.sequence,
            node_id: node_id.clone(),
            group_id: ConsensusGroupId::new(0), // No specific group for decommission
            remaining_groups: 0,
        })
    }
}

#[async_trait]
impl GlobalOperationHandler for NodeOperationHandler {
    type Operation = NodeOperation;
    type Response = NodeOperationResponse;

    async fn apply(
        &self,
        operation: &Self::Operation,
        state: &GlobalState,
        context: &OperationContext,
    ) -> ConsensusResult<NodeOperationResponse> {
        match operation {
            NodeOperation::AssignToGroup { node_id, group_id } => {
                self.handle_assign_to_group(node_id, *group_id, state, context)
                    .await
            }
            NodeOperation::RemoveFromGroup { node_id, group_id } => {
                self.handle_remove_from_group(node_id, *group_id, state, context)
                    .await
            }
            NodeOperation::UpdateGroups { node_id, group_ids } => {
                self.handle_update_groups(node_id, group_ids, state, context)
                    .await
            }
            NodeOperation::Decommission { node_id } => {
                self.handle_decommission(node_id, state, context).await
            }
        }
    }

    async fn post_execute(
        &self,
        operation: &Self::Operation,
        response: &Self::Response,
        _state: &GlobalState,
        _context: &OperationContext,
    ) -> ConsensusResult<()> {
        match response {
            NodeOperationResponse::Failed {
                operation: op,
                reason,
            } => {
                debug!("Failed to execute node operation {}: {}", op, reason);
            }
            _ => {
                debug!("Successfully executed node operation: {:?}", operation);
            }
        }
        Ok(())
    }

    fn operation_type(&self) -> &'static str {
        "NodeOperation"
    }
}
