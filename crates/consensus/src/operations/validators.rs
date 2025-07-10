//! Operation validators
//!
//! This module contains validators for each operation category.
//! Validators check preconditions and ensure operations are valid
//! before they are applied to the state machine.

use crate::{
    error::{ConsensusResult, Error, GroupError, NodeError, StreamError},
    operations::{
        LocalOperationContext, LocalStreamOperation, MaintenanceOperation, MigrationOperation,
        OperationContext, OperationValidator, PubSubOperation, StreamOperation,
        group_ops::GroupOperation, node_ops::NodeOperation, routing_ops::RoutingOperation,
        stream_management_ops::StreamManagementOperation,
    },
};

/// Validator for stream management operations
pub struct StreamManagementOperationValidator;

#[async_trait::async_trait]
impl OperationValidator<StreamManagementOperation> for StreamManagementOperationValidator {
    async fn validate(
        &self,
        operation: &StreamManagementOperation,
        context: &OperationContext<'_>,
    ) -> ConsensusResult<()> {
        // Validate stream name
        StreamManagementOperation::validate_stream_name(operation.stream_name())?;

        match operation {
            StreamManagementOperation::Create {
                name,
                config,
                group_id,
            } => {
                // Check if stream already exists
                if context.global_state.get_stream_config(name).await.is_some() {
                    return Err(Error::Stream(StreamError::AlreadyExists {
                        name: name.to_string(),
                    }));
                }

                // Check if group exists
                if context.global_state.get_group(*group_id).await.is_none() {
                    return Err(Error::Group(GroupError::NotFound { id: *group_id }));
                }

                // Validate stream configuration
                self.validate_stream_config(config)?;

                Ok(())
            }

            StreamManagementOperation::UpdateConfig { name, config } => {
                // Check if stream exists
                context
                    .global_state
                    .get_stream_config(name)
                    .await
                    .ok_or_else(|| {
                        Error::Stream(StreamError::NotFound {
                            name: name.to_string(),
                        })
                    })?;

                // Validate new configuration
                self.validate_stream_config(config)?;

                Ok(())
            }

            StreamManagementOperation::Delete { name } => {
                // Check if stream exists
                context
                    .global_state
                    .get_stream_config(name)
                    .await
                    .ok_or_else(|| {
                        Error::Stream(StreamError::NotFound {
                            name: name.to_string(),
                        })
                    })?;

                // Additional checks could go here (e.g., check if stream is empty)

                Ok(())
            }

            StreamManagementOperation::Reallocate { name, target_group } => {
                // Check if stream exists and get its config
                let stream_config = context
                    .global_state
                    .get_stream_config(name)
                    .await
                    .ok_or_else(|| {
                        Error::Stream(StreamError::NotFound {
                            name: name.to_string(),
                        })
                    })?;

                // Check if target group exists
                if context
                    .global_state
                    .get_group(*target_group)
                    .await
                    .is_none()
                {
                    return Err(Error::Group(GroupError::NotFound { id: *target_group }));
                }

                // Check if already allocated to target
                if stream_config.consensus_group == Some(*target_group) {
                    return Err(Error::InvalidOperation(
                        "Stream is already allocated to the target group".to_string(),
                    ));
                }

                Ok(())
            }

            StreamManagementOperation::Migrate {
                name,
                from_group,
                to_group,
                ..
            } => {
                // Check if stream exists and get its config
                let stream_config = context
                    .global_state
                    .get_stream_config(name)
                    .await
                    .ok_or_else(|| {
                        Error::Stream(StreamError::NotFound {
                            name: name.to_string(),
                        })
                    })?;

                // Verify current allocation
                if stream_config.consensus_group != Some(*from_group) {
                    return Err(Error::InvalidOperation(format!(
                        "Stream is not allocated to group {:?}",
                        from_group
                    )));
                }

                // Check if target group exists
                if context.global_state.get_group(*to_group).await.is_none() {
                    return Err(Error::Group(GroupError::NotFound { id: *to_group }));
                }

                Ok(())
            }

            StreamManagementOperation::UpdateAllocation { name, new_group } => {
                // Check if stream exists
                context
                    .global_state
                    .get_stream_config(name)
                    .await
                    .ok_or_else(|| {
                        Error::Stream(StreamError::NotFound {
                            name: name.to_string(),
                        })
                    })?;

                // Check if group exists
                if context.global_state.get_group(*new_group).await.is_none() {
                    return Err(Error::Group(GroupError::NotFound { id: *new_group }));
                }

                Ok(())
            }
        }
    }
}

impl StreamManagementOperationValidator {
    /// Validate stream configuration
    fn validate_stream_config(&self, config: &crate::global::StreamConfig) -> ConsensusResult<()> {
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

/// Validator for group operations
pub struct GroupOperationValidator {
    /// Minimum nodes required for a group
    pub min_nodes: usize,
    /// Maximum nodes allowed in a group
    pub max_nodes: usize,
}

impl Default for GroupOperationValidator {
    fn default() -> Self {
        Self {
            min_nodes: 1,
            max_nodes: 7,
        }
    }
}

#[async_trait::async_trait]
impl OperationValidator<GroupOperation> for GroupOperationValidator {
    async fn validate(
        &self,
        operation: &GroupOperation,
        context: &OperationContext<'_>,
    ) -> ConsensusResult<()> {
        match operation {
            GroupOperation::Create {
                group_id,
                initial_members,
            } => {
                // Check if group already exists
                if context.global_state.get_group(*group_id).await.is_some() {
                    return Err(Error::Group(GroupError::AlreadyExists { id: *group_id }));
                }

                // Validate member count
                GroupOperation::validate_member_count(
                    initial_members,
                    self.min_nodes,
                    self.max_nodes,
                )?;

                // Check if all nodes exist and are available
                for _node_id in initial_members {
                    // In real implementation, would check node registry
                    // For now, just validate they're not duplicates (done in validate_member_count)
                }

                Ok(())
            }

            GroupOperation::Delete { group_id } => {
                // Check if group exists
                let group = context
                    .global_state
                    .get_group(*group_id)
                    .await
                    .ok_or(Error::Group(GroupError::NotFound { id: *group_id }))?;

                // Check if group has streams
                GroupOperation::can_delete_group(group.stream_count)?;

                Ok(())
            }

            GroupOperation::UpdateMembers { group_id, members } => {
                // Check if group exists
                context
                    .global_state
                    .get_group(*group_id)
                    .await
                    .ok_or(Error::Group(GroupError::NotFound { id: *group_id }))?;

                // Validate new member count
                GroupOperation::validate_member_count(members, self.min_nodes, self.max_nodes)?;

                Ok(())
            }
        }
    }
}

/// Validator for node operations
pub struct NodeOperationValidator {
    /// Maximum groups per node
    pub max_groups_per_node: usize,
}

impl Default for NodeOperationValidator {
    fn default() -> Self {
        Self {
            max_groups_per_node: 10,
        }
    }
}

#[async_trait::async_trait]
impl OperationValidator<NodeOperation> for NodeOperationValidator {
    async fn validate(
        &self,
        operation: &NodeOperation,
        context: &OperationContext<'_>,
    ) -> ConsensusResult<()> {
        match operation {
            NodeOperation::AssignToGroup { node_id, group_id } => {
                // Check if group exists
                let group = context
                    .global_state
                    .get_group(*group_id)
                    .await
                    .ok_or(Error::Group(GroupError::NotFound { id: *group_id }))?;

                // Check if node is already in group
                if group.members.iter().any(|m| m == node_id) {
                    return Err(Error::Node(NodeError::AlreadyInGroup {
                        node_id: node_id.clone(),
                        group_id: *group_id,
                    }));
                }

                // Check node's group limit (would need node registry in real implementation)
                // For now, assume it's within limits

                Ok(())
            }

            NodeOperation::RemoveFromGroup { node_id, group_id } => {
                // Check if group exists
                let group = context
                    .global_state
                    .get_group(*group_id)
                    .await
                    .ok_or(Error::Group(GroupError::NotFound { id: *group_id }))?;

                // Check if node is in group
                if !group.members.iter().any(|m| m == node_id) {
                    return Err(Error::Node(NodeError::NotInGroup {
                        node_id: node_id.clone(),
                        group_id: *group_id,
                    }));
                }

                // Check if removal would leave group with too few members
                NodeOperation::can_remove_from_group(group.members.len(), 1)?;

                Ok(())
            }

            NodeOperation::UpdateGroups { node_id, group_ids } => {
                // Check if all groups exist
                for group_id in group_ids {
                    if context.global_state.get_group(*group_id).await.is_none() {
                        return Err(Error::Group(GroupError::NotFound { id: *group_id }));
                    }
                }

                // Check group limit
                if group_ids.len() > self.max_groups_per_node {
                    return Err(Error::Node(NodeError::TooManyGroups {
                        node_id: node_id.clone(),
                        current: group_ids.len(),
                        max: self.max_groups_per_node,
                    }));
                }

                Ok(())
            }

            NodeOperation::Decommission { node_id } => {
                // Check if node exists in any groups
                let groups = context.global_state.get_all_groups().await;
                let node_groups: Vec<_> = groups
                    .into_iter()
                    .filter(|g| g.members.iter().any(|m| m == node_id))
                    .collect();

                if node_groups.is_empty() {
                    return Err(Error::Node(NodeError::NotFound {
                        id: node_id.clone(),
                    }));
                }

                // Check if removal would leave any group with too few members
                for group in &node_groups {
                    NodeOperation::can_remove_from_group(group.members.len(), 1)?;
                }

                Ok(())
            }
        }
    }
}

/// Validator for routing operations
pub struct RoutingOperationValidator;

#[async_trait::async_trait]
impl OperationValidator<RoutingOperation> for RoutingOperationValidator {
    async fn validate(
        &self,
        operation: &RoutingOperation,
        context: &OperationContext<'_>,
    ) -> ConsensusResult<()> {
        // Check if stream exists
        context
            .global_state
            .get_stream_config(operation.stream_name())
            .await
            .ok_or_else(|| {
                Error::Stream(StreamError::NotFound {
                    name: operation.stream_name().to_string(),
                })
            })?;

        match operation {
            RoutingOperation::Subscribe {
                subject_pattern, ..
            } => {
                RoutingOperation::validate_subject_pattern(subject_pattern)?;

                // Could check for duplicate subscriptions here

                Ok(())
            }

            RoutingOperation::Unsubscribe {
                subject_pattern, ..
            } => {
                RoutingOperation::validate_subject_pattern(subject_pattern)?;

                // Could verify subscription exists here

                Ok(())
            }

            RoutingOperation::RemoveAllSubscriptions { .. } => {
                // No additional validation needed
                Ok(())
            }

            RoutingOperation::BulkSubscribe {
                subject_patterns, ..
            } => {
                for pattern in subject_patterns {
                    RoutingOperation::validate_subject_pattern(pattern)?;
                }

                // Check for duplicates within the bulk operation
                let mut seen = std::collections::HashSet::new();
                for pattern in subject_patterns {
                    if !seen.insert(pattern) {
                        return Err(Error::InvalidOperation(format!(
                            "Duplicate pattern '{}' in bulk subscribe",
                            pattern
                        )));
                    }
                }

                Ok(())
            }

            RoutingOperation::BulkUnsubscribe {
                subject_patterns, ..
            } => {
                for pattern in subject_patterns {
                    RoutingOperation::validate_subject_pattern(pattern)?;
                }

                Ok(())
            }
        }
    }
}

/// Validator for local stream operations
pub struct LocalStreamOperationValidator;

/// Trait for validating local operations
#[async_trait::async_trait]
pub trait LocalOperationValidator<T: Send + Sync> {
    /// Validate the operation in the given context
    async fn validate(
        &self,
        operation: &T,
        context: &LocalOperationContext<'_>,
    ) -> ConsensusResult<()>;
}

#[async_trait::async_trait]
impl LocalOperationValidator<LocalStreamOperation> for LocalStreamOperationValidator {
    async fn validate(
        &self,
        operation: &LocalStreamOperation,
        context: &LocalOperationContext<'_>,
    ) -> ConsensusResult<()> {
        match operation {
            LocalStreamOperation::Stream(op) => self.validate_stream_operation(op, context).await,
            LocalStreamOperation::Migration(op) => {
                self.validate_migration_operation(op, context).await
            }
            LocalStreamOperation::PubSub(op) => self.validate_pubsub_operation(op, context).await,
            LocalStreamOperation::Maintenance(op) => {
                self.validate_maintenance_operation(op, context).await
            }
        }
    }
}

impl LocalStreamOperationValidator {
    /// Validate stream operations
    async fn validate_stream_operation(
        &self,
        operation: &StreamOperation,
        context: &LocalOperationContext<'_>,
    ) -> ConsensusResult<()> {
        let stream_name = operation.stream_name();

        // Check if stream name is valid
        if stream_name.is_empty() {
            return Err(Error::InvalidOperation(
                "Stream name cannot be empty".to_string(),
            ));
        }

        match operation {
            StreamOperation::Publish { .. }
            | StreamOperation::PublishBatch { .. }
            | StreamOperation::Rollup { .. } => {
                // Check if stream exists and is not paused
                match context
                    .local_state
                    .get_stream_metadata(stream_name)
                    .await
                    .map_err(|e| {
                        Error::InvalidOperation(format!("Failed to check stream existence: {}", e))
                    })? {
                    Some(metadata) => {
                        if metadata.is_paused {
                            return Err(Error::InvalidOperation(format!(
                                "Stream '{}' is paused for migration",
                                stream_name
                            )));
                        }
                    }
                    None => {
                        return Err(Error::InvalidOperation(format!(
                            "Stream '{}' does not exist",
                            stream_name
                        )));
                    }
                }
            }
            StreamOperation::Delete { .. } => {
                // Check if stream exists
                if context
                    .local_state
                    .get_stream_metadata(stream_name)
                    .await
                    .map_err(|e| {
                        Error::InvalidOperation(format!("Failed to check stream existence: {}", e))
                    })?
                    .is_none()
                {
                    return Err(Error::InvalidOperation(format!(
                        "Stream '{}' does not exist",
                        stream_name
                    )));
                }
            }
        }

        // Additional validations
        if let StreamOperation::PublishBatch {
            stream: _,
            messages,
        } = operation
        {
            if messages.is_empty() {
                return Err(Error::InvalidOperation("Batch cannot be empty".to_string()));
            }
        }

        if let StreamOperation::Rollup { expected_seq, .. } = operation {
            // Could add validation for expected sequence number
            let _ = expected_seq;
        }

        Ok(())
    }

    /// Validate migration operations
    async fn validate_migration_operation(
        &self,
        operation: &MigrationOperation,
        context: &LocalOperationContext<'_>,
    ) -> ConsensusResult<()> {
        match operation {
            MigrationOperation::CreateStream { stream_name, .. } => {
                // Check if stream already exists
                if context
                    .local_state
                    .get_stream_metadata(stream_name)
                    .await
                    .map_err(|e| {
                        Error::InvalidOperation(format!("Failed to check stream existence: {}", e))
                    })?
                    .is_some()
                {
                    return Err(Error::InvalidOperation(format!(
                        "Stream '{}' already exists",
                        stream_name
                    )));
                }
            }
            MigrationOperation::GetCheckpoint { stream_name }
            | MigrationOperation::GetIncrementalCheckpoint { stream_name, .. }
            | MigrationOperation::PauseStream { stream_name }
            | MigrationOperation::ResumeStream { stream_name }
            | MigrationOperation::RemoveStream { stream_name } => {
                // Check if stream exists
                if context
                    .local_state
                    .get_stream_metadata(stream_name)
                    .await
                    .map_err(|e| {
                        Error::InvalidOperation(format!("Failed to check stream existence: {}", e))
                    })?
                    .is_none()
                {
                    return Err(Error::InvalidOperation(format!(
                        "Stream '{}' does not exist",
                        stream_name
                    )));
                }
            }
            MigrationOperation::ApplyCheckpoint { .. }
            | MigrationOperation::ApplyIncrementalCheckpoint { .. } => {
                // Checkpoint validation happens during application since it requires
                // deserializing and validating the checkpoint data
            }
        }

        Ok(())
    }

    /// Validate PubSub operations
    async fn validate_pubsub_operation(
        &self,
        operation: &PubSubOperation,
        context: &LocalOperationContext<'_>,
    ) -> ConsensusResult<()> {
        match operation {
            PubSubOperation::PublishFromPubSub { stream_name, .. } => {
                // Check if stream exists and is not paused
                match context
                    .local_state
                    .get_stream_metadata(stream_name)
                    .await
                    .map_err(|e| {
                        Error::InvalidOperation(format!("Failed to check stream existence: {}", e))
                    })? {
                    Some(metadata) => {
                        if metadata.is_paused {
                            return Err(Error::InvalidOperation(format!(
                                "Stream '{}' is paused for migration",
                                stream_name
                            )));
                        }
                    }
                    None => {
                        return Err(Error::InvalidOperation(format!(
                            "Stream '{}' does not exist",
                            stream_name
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Validate maintenance operations
    async fn validate_maintenance_operation(
        &self,
        operation: &MaintenanceOperation,
        _context: &LocalOperationContext<'_>,
    ) -> ConsensusResult<()> {
        match operation {
            MaintenanceOperation::GetMetrics => {
                // No validation needed for read operations
            }
            MaintenanceOperation::CleanupPendingOperations { max_age_secs } => {
                if *max_age_secs == 0 {
                    return Err(Error::InvalidOperation(
                        "Max age must be greater than 0".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }
}
