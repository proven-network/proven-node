//! Synchronous validators for operations
//!
//! This module provides synchronous validation wrappers that can be used
//! when async context is not available. These validators use a simple
//! caching strategy to avoid blocking operations.

use crate::{
    error::{ConsensusResult, Error},
    operations::{
        group_ops::GroupOperation, node_ops::NodeOperation, routing_ops::RoutingOperation,
        stream_management_ops::StreamManagementOperation,
    },
};

/// Simple synchronous validator for stream operations
pub struct SyncStreamValidator;

impl SyncStreamValidator {
    /// Validate a stream operation synchronously (basic checks only)
    pub fn validate_sync(operation: &StreamManagementOperation) -> ConsensusResult<()> {
        // Basic validation that doesn't require state lookups
        StreamManagementOperation::validate_stream_name(operation.stream_name())?;

        match operation {
            StreamManagementOperation::Create { config, .. } => {
                // Validate configuration
                Self::validate_stream_config(config)?;
            }
            StreamManagementOperation::UpdateConfig { config, .. } => {
                // Validate configuration
                Self::validate_stream_config(config)?;
            }
            _ => {}
        }

        Ok(())
    }

    fn validate_stream_config(config: &crate::global::StreamConfig) -> ConsensusResult<()> {
        // Validate retention settings based on retention policy
        match &config.retention_policy {
            crate::global::RetentionPolicy::Limits => {
                // No limits is valid - it means retain everything

                // Validate individual limits if set
                if let Some(max_age) = config.max_age_secs {
                    if max_age == 0 {
                        return Err(Error::InvalidOperation(
                            "Retention time must be greater than 0".to_string(),
                        ));
                    }
                }

                if let Some(max_messages) = config.max_messages {
                    if max_messages == 0 {
                        return Err(Error::InvalidOperation(
                            "Retention count must be greater than 0".to_string(),
                        ));
                    }
                }

                if let Some(max_bytes) = config.max_bytes {
                    if max_bytes == 0 {
                        return Err(Error::InvalidOperation(
                            "Retention size must be greater than 0".to_string(),
                        ));
                    }
                }
            }
            crate::global::RetentionPolicy::WorkQueue => {
                // WorkQueue doesn't need specific validation
            }
            crate::global::RetentionPolicy::Interest => {
                // Interest-based retention doesn't need specific validation
            }
        }

        Ok(())
    }
}

/// Simple synchronous validator for group operations
pub struct SyncGroupValidator {
    min_nodes: usize,
    max_nodes: usize,
}

impl Default for SyncGroupValidator {
    fn default() -> Self {
        Self {
            min_nodes: 1,
            max_nodes: 7,
        }
    }
}

impl SyncGroupValidator {
    /// Validate a group operation synchronously (basic checks only)
    pub fn validate_sync(&self, operation: &GroupOperation) -> ConsensusResult<()> {
        match operation {
            GroupOperation::Create {
                initial_members, ..
            } => {
                GroupOperation::validate_member_count(
                    initial_members,
                    self.min_nodes,
                    self.max_nodes,
                )?;
            }
            GroupOperation::UpdateMembers { members, .. } => {
                GroupOperation::validate_member_count(members, self.min_nodes, self.max_nodes)?;
            }
            _ => {}
        }

        Ok(())
    }
}

/// Simple synchronous validator for node operations
pub struct SyncNodeValidator {
    max_groups_per_node: usize,
}

impl Default for SyncNodeValidator {
    fn default() -> Self {
        Self {
            max_groups_per_node: 10,
        }
    }
}

impl SyncNodeValidator {
    /// Validate a node operation synchronously (basic checks only)
    pub fn validate_sync(&self, operation: &NodeOperation) -> ConsensusResult<()> {
        if let NodeOperation::UpdateGroups { node_id, group_ids } = operation {
            if group_ids.len() > self.max_groups_per_node {
                return Err(Error::Node(crate::error::NodeError::TooManyGroups {
                    node_id: node_id.clone(),
                    current: group_ids.len(),
                    max: self.max_groups_per_node,
                }));
            }
        }

        Ok(())
    }
}

/// Simple synchronous validator for routing operations
pub struct SyncRoutingValidator;

impl SyncRoutingValidator {
    /// Validate a routing operation synchronously
    pub fn validate_sync(operation: &RoutingOperation) -> ConsensusResult<()> {
        match operation {
            RoutingOperation::Subscribe {
                subject_pattern, ..
            } => {
                RoutingOperation::validate_subject_pattern(subject_pattern)?;
            }
            RoutingOperation::Unsubscribe {
                subject_pattern, ..
            } => {
                RoutingOperation::validate_subject_pattern(subject_pattern)?;
            }
            RoutingOperation::BulkSubscribe {
                subject_patterns, ..
            } => {
                for pattern in subject_patterns {
                    RoutingOperation::validate_subject_pattern(pattern)?;
                }

                // Check for duplicates
                let mut seen = std::collections::HashSet::new();
                for pattern in subject_patterns {
                    if !seen.insert(pattern) {
                        return Err(Error::InvalidOperation(format!(
                            "Duplicate pattern '{}' in bulk subscribe",
                            pattern
                        )));
                    }
                }
            }
            RoutingOperation::BulkUnsubscribe {
                subject_patterns, ..
            } => {
                for pattern in subject_patterns {
                    RoutingOperation::validate_subject_pattern(pattern)?;
                }
            }
            _ => {}
        }

        Ok(())
    }
}
