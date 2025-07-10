//! Consensus group operations
//!
//! This module contains operations for managing consensus groups,
//! including creation, deletion, and membership updates.

use crate::{
    NodeId,
    allocation::ConsensusGroupId,
    error::{ConsensusResult, Error, GroupError},
};
use serde::{Deserialize, Serialize};

/// Operations related to consensus group management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GroupOperation {
    /// Create a new consensus group
    Create {
        /// Group identifier
        group_id: ConsensusGroupId,
        /// Initial member nodes
        initial_members: Vec<NodeId>,
    },

    /// Delete a consensus group
    Delete {
        /// Group identifier
        group_id: ConsensusGroupId,
    },

    /// Update group membership
    UpdateMembers {
        /// Group identifier
        group_id: ConsensusGroupId,
        /// New member list
        members: Vec<NodeId>,
    },
}

impl GroupOperation {
    /// Get the group ID this operation affects
    pub fn group_id(&self) -> ConsensusGroupId {
        match self {
            Self::Create { group_id, .. }
            | Self::Delete { group_id }
            | Self::UpdateMembers { group_id, .. } => *group_id,
        }
    }

    /// Get a human-readable operation name
    pub fn operation_name(&self) -> String {
        match self {
            Self::Create { .. } => "create_group".to_string(),
            Self::Delete { .. } => "delete_group".to_string(),
            Self::UpdateMembers { .. } => "update_group_members".to_string(),
        }
    }

    /// Check if this operation requires the group to exist
    pub fn requires_existing_group(&self) -> bool {
        !matches!(self, Self::Create { .. })
    }

    /// Check if this operation modifies membership
    pub fn modifies_membership(&self) -> bool {
        matches!(self, Self::Create { .. } | Self::UpdateMembers { .. })
    }

    /// Validate member count for a group
    pub fn validate_member_count(
        members: &[NodeId],
        min: usize,
        max: usize,
    ) -> ConsensusResult<()> {
        let count = members.len();

        if count < min {
            return Err(Error::Group(GroupError::InsufficientMembers {
                required: min,
                actual: count,
            }));
        }

        if count > max {
            return Err(Error::Group(GroupError::TooManyMembers {
                max,
                actual: count,
            }));
        }

        // Check for duplicates
        let mut unique = std::collections::HashSet::new();
        for node_id in members {
            if !unique.insert(node_id) {
                return Err(Error::InvalidOperation(format!(
                    "Duplicate node {} in member list",
                    node_id
                )));
            }
        }

        Ok(())
    }

    /// Check if a group can be safely deleted
    pub fn can_delete_group(stream_count: usize) -> ConsensusResult<()> {
        if stream_count > 0 {
            return Err(Error::Group(GroupError::HasActiveStreams {
                id: ConsensusGroupId::new(0), // Will be filled by validator
                count: stream_count,
            }));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_member_validation() {
        let node1 = NodeId::from_seed(1);
        let node2 = NodeId::from_seed(2);
        let node3 = NodeId::from_seed(3);

        // Valid member count
        assert!(
            GroupOperation::validate_member_count(&[node1.clone(), node2.clone()], 1, 3).is_ok()
        );

        // Too few members
        assert!(GroupOperation::validate_member_count(&[], 1, 3).is_err());

        // Too many members
        assert!(
            GroupOperation::validate_member_count(
                &[
                    node1.clone(),
                    node2.clone(),
                    node3.clone(),
                    NodeId::from_seed(4)
                ],
                1,
                3
            )
            .is_err()
        );

        // Duplicate members
        assert!(
            GroupOperation::validate_member_count(&[node1.clone(), node1.clone()], 1, 3).is_err()
        );
    }

    #[test]
    fn test_operation_properties() {
        let create_op = GroupOperation::Create {
            group_id: ConsensusGroupId::new(1),
            initial_members: vec![NodeId::from_seed(1)],
        };

        assert_eq!(create_op.group_id(), ConsensusGroupId::new(1));
        assert_eq!(create_op.operation_name(), "create_group");
        assert!(!create_op.requires_existing_group());
        assert!(create_op.modifies_membership());
    }
}
