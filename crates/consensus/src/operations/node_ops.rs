//! Node management operations
//!
//! This module contains operations for managing nodes within the consensus system,
//! including group assignments and decommissioning.

use crate::{
    ConsensusGroupId, NodeId,
    error::{ConsensusResult, Error},
};
use serde::{Deserialize, Serialize};

/// Operations related to node management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeOperation {
    /// Assign a node to a consensus group
    AssignToGroup {
        /// Node identifier
        node_id: NodeId,
        /// Target group
        group_id: ConsensusGroupId,
    },

    /// Remove a node from a consensus group
    RemoveFromGroup {
        /// Node identifier
        node_id: NodeId,
        /// Source group
        group_id: ConsensusGroupId,
    },

    /// Update all group assignments for a node
    UpdateGroups {
        /// Node identifier
        node_id: NodeId,
        /// New set of groups
        group_ids: Vec<ConsensusGroupId>,
    },

    /// Decommission a node (remove from all groups)
    Decommission {
        /// Node identifier
        node_id: NodeId,
    },
}

impl NodeOperation {
    /// Get the node ID this operation affects
    pub fn node_id(&self) -> &NodeId {
        match self {
            Self::AssignToGroup { node_id, .. }
            | Self::RemoveFromGroup { node_id, .. }
            | Self::UpdateGroups { node_id, .. }
            | Self::Decommission { node_id } => node_id,
        }
    }

    /// Get a human-readable operation name
    pub fn operation_name(&self) -> String {
        match self {
            Self::AssignToGroup { .. } => "assign_node_to_group".to_string(),
            Self::RemoveFromGroup { .. } => "remove_node_from_group".to_string(),
            Self::UpdateGroups { .. } => "update_node_groups".to_string(),
            Self::Decommission { .. } => "decommission_node".to_string(),
        }
    }

    /// Check if this operation adds the node to groups
    pub fn adds_to_groups(&self) -> bool {
        matches!(self, Self::AssignToGroup { .. } | Self::UpdateGroups { .. })
    }

    /// Check if this operation removes the node from groups
    pub fn removes_from_groups(&self) -> bool {
        matches!(
            self,
            Self::RemoveFromGroup { .. } | Self::UpdateGroups { .. } | Self::Decommission { .. }
        )
    }

    /// Validate group assignment limits
    pub fn validate_group_limit(
        current_count: usize,
        max_groups: usize,
        adding: usize,
    ) -> ConsensusResult<()> {
        if current_count + adding > max_groups {
            // Note: node_id should be provided by the caller
            return Err(Error::InvalidOperation(format!(
                "Too many groups: current {current_count}, max {max_groups}, trying to add {adding}"
            )));
        }
        Ok(())
    }

    /// Check if node can be safely removed from a group
    pub fn can_remove_from_group(
        group_member_count: usize,
        min_members: usize,
    ) -> ConsensusResult<()> {
        if group_member_count <= min_members {
            return Err(Error::InvalidOperation(format!(
                "Removing node would leave group with {} members, below minimum of {}",
                group_member_count - 1,
                min_members
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_limit_validation() {
        // Within limit
        assert!(NodeOperation::validate_group_limit(2, 5, 2).is_ok());

        // At limit
        assert!(NodeOperation::validate_group_limit(3, 5, 2).is_ok());

        // Over limit
        assert!(NodeOperation::validate_group_limit(4, 5, 2).is_err());
    }

    #[test]
    fn test_can_remove_from_group() {
        // Can remove
        assert!(NodeOperation::can_remove_from_group(4, 3).is_ok());

        // At minimum
        assert!(NodeOperation::can_remove_from_group(3, 3).is_err());

        // Below minimum
        assert!(NodeOperation::can_remove_from_group(2, 3).is_err());
    }

    #[test]
    fn test_operation_properties() {
        let node_id = NodeId::from_seed(1);

        let assign_op = NodeOperation::AssignToGroup {
            node_id: node_id.clone(),
            group_id: ConsensusGroupId::new(1),
        };

        assert_eq!(assign_op.node_id(), &node_id);
        assert_eq!(assign_op.operation_name(), "assign_node_to_group");
        assert!(assign_op.adds_to_groups());
        assert!(!assign_op.removes_from_groups());

        let decommission_op = NodeOperation::Decommission {
            node_id: node_id.clone(),
        };

        assert!(!decommission_op.adds_to_groups());
        assert!(decommission_op.removes_from_groups());
    }
}
