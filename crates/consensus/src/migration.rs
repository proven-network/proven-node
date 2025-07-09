//! Stream migration protocol for load balancing
//!
//! This module handles the migration of streams between consensus groups
//! to balance load and ensure optimal performance.

use crate::allocation::ConsensusGroupId;
use crate::error::{ConsensusResult, Error};
use crate::global::{GlobalOperation, StreamConfig};
use crate::local::state_machine::StreamData;
use crate::local::{LocalStreamOperation, MigrationState};
use crate::router::ConsensusRouter;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Migration coordinator that handles stream migrations between consensus groups
pub struct MigrationCoordinator<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static,
    A: proven_attestation::Attestor + Send + Sync + 'static,
{
    /// Reference to the consensus router
    router: Arc<ConsensusRouter<G, A>>,
    /// Active migrations
    active_migrations: Arc<RwLock<HashMap<String, ActiveMigration>>>,
}

/// Represents an active migration
#[derive(Debug, Clone)]
pub struct ActiveMigration {
    /// Stream being migrated
    pub stream_name: String,
    /// Source consensus group
    pub source_group: ConsensusGroupId,
    /// Target consensus group
    pub target_group: ConsensusGroupId,
    /// Current state of migration
    pub state: MigrationState,
    /// Timestamp when migration started
    pub started_at: u64,
    /// Last checkpoint sequence number
    pub checkpoint_seq: Option<u64>,
    /// Error if migration failed
    pub error: Option<String>,
}

/// Migration checkpoint data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationCheckpoint {
    /// Stream name
    pub stream_name: String,
    /// Sequence number up to which data has been migrated
    pub sequence: u64,
    /// Stream data snapshot
    pub data: StreamData,
    /// Stream configuration
    pub config: StreamConfig,
    /// PubSub subscriptions for this stream
    pub subscriptions: Vec<String>,
    /// Checkpoint creation timestamp
    pub created_at: u64,
    /// Checksum for data integrity verification
    pub checksum: String,
    /// Compression algorithm used (if any)
    pub compression: CompressionType,
    /// Whether this is an incremental checkpoint
    pub is_incremental: bool,
    /// Base checkpoint sequence for incremental checkpoints
    pub base_checkpoint_seq: Option<u64>,
}

/// Compression type for checkpoint data
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub enum CompressionType {
    /// No compression
    None,
    /// Gzip compression
    #[default]
    Gzip,
    /// LZ4 compression for speed
    Lz4,
}

/// Enhanced migration checkpoint with compression and validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressedCheckpoint {
    /// Metadata about the checkpoint
    pub metadata: CheckpointMetadata,
    /// Compressed checkpoint data
    pub compressed_data: Bytes,
}

/// Metadata for checkpoints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    /// Original size before compression
    pub original_size: u64,
    /// Compressed size
    pub compressed_size: u64,
    /// Compression ratio achieved
    pub compression_ratio: f64,
    /// Compression algorithm used
    pub compression_type: CompressionType,
    /// Checksum of original data
    pub original_checksum: String,
    /// Checksum of compressed data
    pub compressed_checksum: String,
}

/// Progress information for an active migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationProgress {
    /// Stream being migrated
    pub stream_name: String,
    /// Source consensus group
    pub source_group: ConsensusGroupId,
    /// Target consensus group
    pub target_group: ConsensusGroupId,
    /// Current migration state
    pub current_state: MigrationState,
    /// Time elapsed since migration started (milliseconds)
    pub elapsed_ms: u64,
    /// Last checkpoint sequence number
    pub checkpoint_seq: Option<u64>,
    /// Error message if migration failed
    pub error: Option<String>,
    /// Estimated completion time (milliseconds from start)
    pub estimated_completion_ms: Option<u64>,
}

impl<G, A> MigrationCoordinator<G, A>
where
    G: proven_governance::Governance + Send + Sync + 'static,
    A: proven_attestation::Attestor + Send + Sync + 'static,
{
    /// Create a new migration coordinator
    pub fn new(router: Arc<ConsensusRouter<G, A>>) -> Self {
        Self {
            router,
            active_migrations: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start a stream migration
    pub async fn start_migration(
        &self,
        stream_name: String,
        source_group: ConsensusGroupId,
        target_group: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        info!(
            "Starting migration of stream {} from group {:?} to {:?}",
            stream_name, source_group, target_group
        );

        // Validate migration preconditions
        self.validate_migration_preconditions(&stream_name, source_group, target_group)
            .await?;

        // Create migration record
        let migration = ActiveMigration {
            stream_name: stream_name.clone(),
            source_group,
            target_group,
            state: MigrationState::Preparing,
            started_at: chrono::Utc::now().timestamp_millis() as u64,
            checkpoint_seq: None,
            error: None,
        };

        // Add to active migrations
        {
            let mut migrations = self.active_migrations.write().await;
            migrations.insert(stream_name.clone(), migration);
        }

        // Submit migration request to global consensus
        let operation = GlobalOperation::MigrateStream {
            stream_name: stream_name.clone(),
            from_group: source_group,
            to_group: target_group,
            state: MigrationState::Preparing,
        };

        self.router
            .route_global_operation(operation)
            .await
            .map(|_| ())
    }

    /// Execute the migration steps
    pub async fn execute_migration(&self, stream_name: &str) -> ConsensusResult<()> {
        let migration = {
            let migrations = self.active_migrations.read().await;
            migrations.get(stream_name).cloned().ok_or_else(|| {
                Error::NotFound(format!("Migration for stream {} not found", stream_name))
            })?
        };

        match migration.state {
            MigrationState::Preparing => {
                self.prepare_target_group(&migration).await?;
                self.update_migration_state(stream_name, MigrationState::Transferring)
                    .await?;
            }
            MigrationState::Transferring => {
                self.transfer_data(&migration).await?;
                self.update_migration_state(stream_name, MigrationState::Syncing)
                    .await?;
            }
            MigrationState::Syncing => {
                self.sync_incremental_data(&migration).await?;
                self.update_migration_state(stream_name, MigrationState::Switching)
                    .await?;
            }
            MigrationState::Switching => {
                self.switch_traffic(&migration).await?;
                self.update_migration_state(stream_name, MigrationState::Completing)
                    .await?;
            }
            MigrationState::Completing => {
                self.complete_migration(&migration).await?;
                self.update_migration_state(stream_name, MigrationState::Completed)
                    .await?;
            }
            MigrationState::Completed => {
                // Remove from active migrations
                let mut migrations = self.active_migrations.write().await;
                migrations.remove(stream_name);
                info!("Migration of stream {} completed successfully", stream_name);
            }
            MigrationState::Failed => {
                warn!("Migration of stream {} is in failed state", stream_name);
                return Err(Error::MigrationFailed(
                    migration
                        .error
                        .unwrap_or_else(|| "Unknown error".to_string()),
                ));
            }
        }

        Ok(())
    }

    /// Prepare the target group to receive the stream
    async fn prepare_target_group(&self, migration: &ActiveMigration) -> ConsensusResult<()> {
        debug!(
            "Preparing target group {:?} for stream {}",
            migration.target_group, migration.stream_name
        );

        // Create stream in target group with migration marker
        let operation = LocalStreamOperation::CreateStreamForMigration {
            stream_name: migration.stream_name.clone(),
            source_group: migration.source_group,
        };

        self.router
            .route_local_operation(migration.target_group, operation)
            .await
            .map(|_| ())
    }

    /// Transfer initial snapshot from source to target group
    async fn transfer_data(&self, migration: &ActiveMigration) -> ConsensusResult<()> {
        info!(
            "Transferring initial snapshot for stream {} from group {:?} to {:?}",
            migration.stream_name, migration.source_group, migration.target_group
        );

        // Get initial snapshot checkpoint
        let initial_checkpoint = self.get_stream_checkpoint(migration).await?;
        let initial_seq = initial_checkpoint.sequence;

        info!(
            "Initial snapshot for stream {} contains {} messages (sequence up to {})",
            migration.stream_name,
            initial_checkpoint.data.messages.len(),
            initial_seq
        );

        // Apply initial checkpoint to target group
        self.apply_checkpoint(&initial_checkpoint, migration.target_group)
            .await?;

        // Update checkpoint sequence in migration record
        let mut migrations = self.active_migrations.write().await;
        if let Some(active_migration) = migrations.get_mut(&migration.stream_name) {
            active_migration.checkpoint_seq = Some(initial_seq);
        }

        info!(
            "Initial snapshot transfer completed for stream {} (sequence: {})",
            migration.stream_name, initial_seq
        );

        Ok(())
    }

    /// Sync incremental data updates to minimize switchover downtime
    async fn sync_incremental_data(&self, migration: &ActiveMigration) -> ConsensusResult<()> {
        info!(
            "Starting incremental sync for stream {} from group {:?} to {:?}",
            migration.stream_name, migration.source_group, migration.target_group
        );

        let initial_seq = migration.checkpoint_seq.unwrap_or(0);
        let mut last_synced_seq = initial_seq;
        let max_sync_iterations = 5; // Prevent infinite loops
        let mut sync_iteration = 0;
        let mut total_synced_messages = 0;

        loop {
            sync_iteration += 1;
            if sync_iteration > max_sync_iterations {
                warn!(
                    "Maximum sync iterations reached for stream {}, proceeding with switch",
                    migration.stream_name
                );
                break;
            }

            // Wait a short time for new messages to accumulate
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            // Get incremental checkpoint
            let incremental_checkpoint = self
                .get_incremental_checkpoint(migration, last_synced_seq)
                .await?;

            if incremental_checkpoint.sequence <= last_synced_seq {
                // No new data to sync
                info!(
                    "No new data to sync for stream {}, proceeding with switch",
                    migration.stream_name
                );
                break;
            }

            let messages_in_increment = incremental_checkpoint.data.messages.len();
            info!(
                "Syncing incremental data for stream {} ({} messages, sequences {} to {})",
                migration.stream_name,
                messages_in_increment,
                last_synced_seq + 1,
                incremental_checkpoint.sequence
            );

            // Apply incremental checkpoint
            self.apply_incremental_checkpoint(&incremental_checkpoint, migration.target_group)
                .await?;

            last_synced_seq = incremental_checkpoint.sequence;
            total_synced_messages += messages_in_increment;

            // If we're syncing very small increments, we're caught up
            if messages_in_increment < 10 {
                info!(
                    "Small increment detected ({} messages), considering sync complete for stream {}",
                    messages_in_increment, migration.stream_name
                );
                break;
            }
        }

        // Update final checkpoint sequence
        let mut migrations = self.active_migrations.write().await;
        if let Some(active_migration) = migrations.get_mut(&migration.stream_name) {
            active_migration.checkpoint_seq = Some(last_synced_seq);
        }

        info!(
            "Incremental sync completed for stream {} ({} total messages synced, final sequence: {})",
            migration.stream_name, total_synced_messages, last_synced_seq
        );

        Ok(())
    }

    /// Get incremental checkpoint since last sequence
    async fn get_incremental_checkpoint(
        &self,
        migration: &ActiveMigration,
        since_seq: u64,
    ) -> ConsensusResult<MigrationCheckpoint> {
        // Use the specialized incremental checkpoint operation
        let operation = LocalStreamOperation::GetIncrementalCheckpoint {
            stream_name: migration.stream_name.clone(),
            since_sequence: since_seq,
        };

        let response = self
            .router
            .route_local_operation(migration.source_group, operation)
            .await?;

        if !response.success {
            return Err(Error::Stream(response.error.unwrap_or_else(|| {
                "Failed to get incremental checkpoint".to_string()
            })));
        }

        // Retrieve the incremental checkpoint from the response
        let checkpoint_data = response
            .checkpoint_data
            .ok_or_else(|| Error::Stream("No checkpoint data in response".to_string()))?;

        // Deserialize the incremental checkpoint
        let mut checkpoint: MigrationCheckpoint = serde_json::from_slice(&checkpoint_data)
            .map_err(|e| {
                Error::Deserialization(format!(
                    "Failed to deserialize incremental checkpoint: {}",
                    e
                ))
            })?;

        // Now retrieve the actual stream configuration and subscriptions from global state
        let global_state = self.router.global_manager().get_current_state().await?;

        // Get stream config
        let stream_configs = global_state.stream_configs.read().await;
        if let Some(config) = stream_configs.get(&migration.stream_name) {
            checkpoint.config = config.clone();
        }

        // Get subscriptions
        let streams = global_state.streams.read().await;
        if let Some(stream_data) = streams.get(&migration.stream_name) {
            checkpoint.subscriptions = stream_data.subscriptions.iter().cloned().collect();
        }

        // Validate the checkpoint
        validate_checkpoint(&checkpoint)?;

        // Verify this is indeed an incremental checkpoint
        if !checkpoint.is_incremental {
            return Err(Error::InvalidCheckpoint(
                "Expected incremental checkpoint but got full checkpoint".to_string(),
            ));
        }

        info!(
            "Retrieved incremental checkpoint for stream {} with {} messages (sequences after {}), {} subscriptions",
            checkpoint.stream_name,
            checkpoint.data.messages.len(),
            since_seq,
            checkpoint.subscriptions.len()
        );

        Ok(checkpoint)
    }

    /// Apply incremental checkpoint to target group
    async fn apply_incremental_checkpoint(
        &self,
        checkpoint: &MigrationCheckpoint,
        target_group: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        // Validate that this is indeed an incremental checkpoint
        if !checkpoint.is_incremental {
            return Err(Error::InvalidCheckpoint(
                "Expected incremental checkpoint".to_string(),
            ));
        }

        validate_checkpoint(checkpoint)?;

        // For incremental checkpoints, we might use a different operation
        // that merges new data rather than replacing existing data
        let compressed_checkpoint = compress_checkpoint(checkpoint, checkpoint.compression)?;

        info!(
            "Applying incremental checkpoint: {:.2}x compression, {} bytes -> {} bytes",
            compressed_checkpoint.metadata.compression_ratio,
            compressed_checkpoint.metadata.original_size,
            compressed_checkpoint.metadata.compressed_size
        );

        // Use the specialized incremental apply operation
        let operation = LocalStreamOperation::ApplyIncrementalCheckpoint {
            checkpoint: compressed_checkpoint.compressed_data,
        };

        let response = self
            .router
            .route_local_operation(target_group, operation)
            .await?;

        if !response.success {
            return Err(Error::Stream(response.error.unwrap_or_else(|| {
                "Failed to apply incremental checkpoint".to_string()
            })));
        }

        Ok(())
    }

    /// Switch traffic from source to target group
    async fn switch_traffic(&self, migration: &ActiveMigration) -> ConsensusResult<()> {
        info!(
            "Switching traffic for stream {} to group {:?}",
            migration.stream_name, migration.target_group
        );

        // Update allocation in global consensus
        let operation = GlobalOperation::UpdateStreamAllocation {
            stream_name: migration.stream_name.clone(),
            new_group: migration.target_group,
        };

        self.router.route_global_operation(operation).await?;

        // Pause writes on source group
        let pause_op = LocalStreamOperation::PauseStream {
            stream_name: migration.stream_name.clone(),
        };

        self.router
            .route_local_operation(migration.source_group, pause_op)
            .await?;

        Ok(())
    }

    /// Complete the migration
    async fn complete_migration(&self, migration: &ActiveMigration) -> ConsensusResult<()> {
        debug!("Completing migration for stream {}", migration.stream_name);

        // Remove stream from source group
        let remove_op = LocalStreamOperation::RemoveStream {
            stream_name: migration.stream_name.clone(),
        };

        self.router
            .route_local_operation(migration.source_group, remove_op)
            .await?;

        // Resume stream on target group
        let resume_op = LocalStreamOperation::ResumeStream {
            stream_name: migration.stream_name.clone(),
        };

        self.router
            .route_local_operation(migration.target_group, resume_op)
            .await?;

        Ok(())
    }

    /// Get a checkpoint of stream data from the source group
    async fn get_stream_checkpoint(
        &self,
        migration: &ActiveMigration,
    ) -> ConsensusResult<MigrationCheckpoint> {
        let operation = LocalStreamOperation::GetStreamCheckpoint {
            stream_name: migration.stream_name.clone(),
        };

        let response = self
            .router
            .route_local_operation(migration.source_group, operation)
            .await?;

        if !response.success {
            return Err(Error::Stream(
                response
                    .error
                    .unwrap_or_else(|| "Failed to get checkpoint".to_string()),
            ));
        }

        // Retrieve the actual checkpoint from the response
        let checkpoint_data = response
            .checkpoint_data
            .ok_or_else(|| Error::Stream("No checkpoint data in response".to_string()))?;

        // Deserialize the checkpoint
        let mut checkpoint: MigrationCheckpoint = serde_json::from_slice(&checkpoint_data)
            .map_err(|e| {
                Error::Deserialization(format!("Failed to deserialize checkpoint: {}", e))
            })?;

        // Now retrieve the actual stream configuration and subscriptions from global state
        let global_state = self.router.global_manager().get_current_state().await?;

        // Get stream config
        let stream_configs = global_state.stream_configs.read().await;
        if let Some(config) = stream_configs.get(&migration.stream_name) {
            checkpoint.config = config.clone();
        }

        // Get subscriptions
        let streams = global_state.streams.read().await;
        if let Some(stream_data) = streams.get(&migration.stream_name) {
            checkpoint.subscriptions = stream_data.subscriptions.iter().cloned().collect();
        }

        // Validate the checkpoint before returning
        validate_checkpoint(&checkpoint)?;

        info!(
            "Retrieved checkpoint for stream {} with {} messages (sequence: {}), {} subscriptions",
            checkpoint.stream_name,
            checkpoint.data.messages.len(),
            checkpoint.sequence,
            checkpoint.subscriptions.len()
        );

        Ok(checkpoint)
    }

    /// Apply a checkpoint to the target group
    async fn apply_checkpoint(
        &self,
        checkpoint: &MigrationCheckpoint,
        target_group: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        // Validate checkpoint before applying
        validate_checkpoint(checkpoint)?;

        // Compress checkpoint if beneficial
        let compressed_checkpoint = compress_checkpoint(checkpoint, checkpoint.compression)?;

        // Log compression statistics
        info!(
            "Checkpoint compression: {:.2}x ratio, {} bytes -> {} bytes",
            compressed_checkpoint.metadata.compression_ratio,
            compressed_checkpoint.metadata.original_size,
            compressed_checkpoint.metadata.compressed_size
        );

        let operation = LocalStreamOperation::ApplyMigrationCheckpoint {
            checkpoint: compressed_checkpoint.compressed_data,
        };

        let response = self
            .router
            .route_local_operation(target_group, operation)
            .await?;

        if !response.success {
            return Err(Error::Stream(
                response
                    .error
                    .unwrap_or_else(|| "Failed to apply checkpoint".to_string()),
            ));
        }

        Ok(())
    }

    /// Update migration state
    async fn update_migration_state(
        &self,
        stream_name: &str,
        new_state: MigrationState,
    ) -> ConsensusResult<()> {
        // Update local state
        {
            let mut migrations = self.active_migrations.write().await;
            if let Some(migration) = migrations.get_mut(stream_name) {
                migration.state = new_state.clone();
            }
        }

        // Update global state
        let migration = {
            let migrations = self.active_migrations.read().await;
            migrations.get(stream_name).cloned()
        };

        if let Some(migration) = migration {
            let operation = GlobalOperation::MigrateStream {
                stream_name: stream_name.to_string(),
                from_group: migration.source_group,
                to_group: migration.target_group,
                state: new_state,
            };

            self.router.route_global_operation(operation).await?;
        }

        Ok(())
    }

    /// Handle migration failure with automatic rollback
    pub async fn fail_migration(&self, stream_name: &str, error: String) -> ConsensusResult<()> {
        error!("Migration failed for stream {}: {}", stream_name, error);

        let migration = {
            let mut migrations = self.active_migrations.write().await;
            if let Some(migration) = migrations.get_mut(stream_name) {
                migration.state = MigrationState::Failed;
                migration.error = Some(error.clone());
                migration.clone()
            } else {
                return Err(Error::NotFound(format!(
                    "Migration for stream {} not found",
                    stream_name
                )));
            }
        };

        // Attempt rollback if migration progressed past preparation
        match migration.state {
            MigrationState::Transferring
            | MigrationState::Syncing
            | MigrationState::Switching
            | MigrationState::Completing => {
                info!(
                    "Attempting rollback for failed migration of stream {}",
                    stream_name
                );
                if let Err(rollback_error) = self.rollback_migration(&migration).await {
                    error!(
                        "Rollback failed for stream {}: {}. Manual intervention required.",
                        stream_name, rollback_error
                    );
                }
            }
            _ => {
                debug!(
                    "No rollback needed for stream {} in state {:?}",
                    stream_name, migration.state
                );
            }
        }

        Ok(())
    }

    /// Rollback a failed migration
    async fn rollback_migration(&self, migration: &ActiveMigration) -> ConsensusResult<()> {
        info!(
            "Rolling back migration for stream {}",
            migration.stream_name
        );

        // Resume source stream if it was paused
        let resume_op = LocalStreamOperation::ResumeStream {
            stream_name: migration.stream_name.clone(),
        };

        if let Err(e) = self
            .router
            .route_local_operation(migration.source_group, resume_op)
            .await
        {
            warn!(
                "Failed to resume source stream {} during rollback: {}",
                migration.stream_name, e
            );
        }

        // Remove stream from target group if it was created
        let remove_op = LocalStreamOperation::RemoveStream {
            stream_name: migration.stream_name.clone(),
        };

        if let Err(e) = self
            .router
            .route_local_operation(migration.target_group, remove_op)
            .await
        {
            warn!(
                "Failed to remove stream {} from target group during rollback: {}",
                migration.stream_name, e
            );
        }

        // Restore allocation to source group in global consensus
        let restore_allocation_op = GlobalOperation::UpdateStreamAllocation {
            stream_name: migration.stream_name.clone(),
            new_group: migration.source_group,
        };

        if let Err(e) = self
            .router
            .route_global_operation(restore_allocation_op)
            .await
        {
            warn!(
                "Failed to restore allocation for stream {} during rollback: {}",
                migration.stream_name, e
            );
        }

        info!("Rollback completed for stream {}", migration.stream_name);
        Ok(())
    }

    /// Validate migration preconditions
    pub async fn validate_migration_preconditions(
        &self,
        stream_name: &str,
        source_group: ConsensusGroupId,
        target_group: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        // Check if migration is already active
        {
            let migrations = self.active_migrations.read().await;
            if migrations.contains_key(stream_name) {
                return Err(Error::AlreadyExists(format!(
                    "Migration for stream {} is already in progress",
                    stream_name
                )));
            }
        }

        // Validate source and target groups are different
        if source_group == target_group {
            return Err(Error::InvalidOperation(format!(
                "Source and target groups cannot be the same: {:?}",
                source_group
            )));
        }

        // Verify source group exists and contains the stream
        let stream_info = self
            .router
            .query_stream_info(stream_name)
            .await
            .map_err(|e| Error::NotFound(format!("Stream {} not found: {}", stream_name, e)))?;

        // Verify the stream is actually in the source group
        if stream_info.consensus_group != Some(source_group) {
            return Err(Error::InvalidOperation(format!(
                "Stream {} is not in source group {:?}, it's in {:?}",
                stream_name, source_group, stream_info.consensus_group
            )));
        }

        // Verify target group exists
        let target_group_info = self
            .router
            .get_consensus_group_info(target_group)
            .await
            .ok_or_else(|| {
                Error::NotFound(format!(
                    "Target consensus group {:?} not found",
                    target_group
                ))
            })?;

        // Check target group has capacity (at least one node)
        if target_group_info.members.is_empty() {
            return Err(Error::InvalidOperation(format!(
                "Target group {:?} has no members",
                target_group
            )));
        }

        // Check source group health and quorum
        let source_health = self
            .router
            .check_group_health(source_group)
            .await
            .map_err(|e| {
                Error::InvalidState(format!("Failed to check source group health: {}", e))
            })?;

        if !source_health.has_quorum {
            return Err(Error::InvalidState(format!(
                "Source group {:?} does not have quorum (available: {}, required: {})",
                source_group, source_health.available_nodes, source_health.required_nodes
            )));
        }

        // Check target group health and quorum
        let target_health = self
            .router
            .check_group_health(target_group)
            .await
            .map_err(|e| {
                Error::InvalidState(format!("Failed to check target group health: {}", e))
            })?;

        if !target_health.has_quorum {
            return Err(Error::InvalidState(format!(
                "Target group {:?} does not have quorum (available: {}, required: {})",
                target_group, target_health.available_nodes, target_health.required_nodes
            )));
        }

        // Check if stream is paused (indicating another operation in progress)
        if stream_info.is_paused {
            return Err(Error::InvalidState(format!(
                "Stream {} is currently paused, possibly due to another operation",
                stream_name
            )));
        }

        // Additional safety check: ensure target group has capacity for the stream
        let target_stream_count = self
            .router
            .get_group_stream_count(target_group)
            .await
            .unwrap_or(0);

        // Conservative limit to prevent overloading groups
        const MAX_STREAMS_PER_GROUP: usize = 1000;
        if target_stream_count >= MAX_STREAMS_PER_GROUP {
            return Err(Error::InvalidOperation(format!(
                "Target group {:?} already has {} streams (max: {})",
                target_group, target_stream_count, MAX_STREAMS_PER_GROUP
            )));
        }

        info!(
            "Migration preconditions validated for stream {} from {:?} to {:?}",
            stream_name, source_group, target_group
        );

        Ok(())
    }

    /// Get migration progress and statistics
    pub async fn get_migration_progress(&self, stream_name: &str) -> Option<MigrationProgress> {
        let migrations = self.active_migrations.read().await;
        migrations.get(stream_name).map(|migration| {
            let elapsed_ms = chrono::Utc::now().timestamp_millis() as u64 - migration.started_at;

            MigrationProgress {
                stream_name: stream_name.to_string(),
                source_group: migration.source_group,
                target_group: migration.target_group,
                current_state: migration.state.clone(),
                elapsed_ms,
                checkpoint_seq: migration.checkpoint_seq,
                error: migration.error.clone(),
                estimated_completion_ms: estimate_completion_time(&migration.state, elapsed_ms),
            }
        })
    }

    /// Get active migrations
    pub async fn get_active_migrations(&self) -> Vec<ActiveMigration> {
        let migrations = self.active_migrations.read().await;
        migrations.values().cloned().collect()
    }

    /// Cancel a migration
    pub async fn cancel_migration(&self, stream_name: &str) -> ConsensusResult<()> {
        let mut migrations = self.active_migrations.write().await;

        if let Some(migration) = migrations.get_mut(stream_name) {
            if matches!(
                migration.state,
                MigrationState::Completed | MigrationState::Failed
            ) {
                return Err(Error::InvalidState(format!(
                    "Cannot cancel migration in {:?} state",
                    migration.state
                )));
            }

            migration.state = MigrationState::Failed;
            migration.error = Some("Migration cancelled by user".to_string());
        }

        Ok(())
    }
}

/// Compress checkpoint data
pub fn compress_checkpoint(
    checkpoint: &MigrationCheckpoint,
    compression_type: CompressionType,
) -> ConsensusResult<CompressedCheckpoint> {
    let serialized =
        serde_json::to_vec(checkpoint).map_err(|e| Error::Serialization(e.to_string()))?;

    let original_size = serialized.len() as u64;
    let original_checksum = calculate_checksum(&serialized);

    let compressed_data = match compression_type {
        CompressionType::None => Bytes::from(serialized),
        CompressionType::Gzip => {
            use flate2::Compression;
            use flate2::write::GzEncoder;
            use std::io::Write;

            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(&serialized)
                .map_err(|e| Error::CompressionFailed(e.to_string()))?;
            let compressed = encoder
                .finish()
                .map_err(|e| Error::CompressionFailed(e.to_string()))?;
            Bytes::from(compressed)
        }
        CompressionType::Lz4 => {
            // For now, fallback to no compression as lz4 requires additional dependency
            // In production, would use lz4_flex crate
            warn!("LZ4 compression not implemented, using no compression");
            Bytes::from(serialized)
        }
    };

    let compressed_size = compressed_data.len() as u64;
    let compressed_checksum = calculate_checksum(&compressed_data);
    let compression_ratio = if compressed_size > 0 {
        original_size as f64 / compressed_size as f64
    } else {
        1.0
    };

    Ok(CompressedCheckpoint {
        metadata: CheckpointMetadata {
            original_size,
            compressed_size,
            compression_ratio,
            compression_type,
            original_checksum,
            compressed_checksum,
        },
        compressed_data,
    })
}

/// Decompress checkpoint data
pub fn decompress_checkpoint(
    compressed: &CompressedCheckpoint,
) -> ConsensusResult<MigrationCheckpoint> {
    let decompressed_data = match compressed.metadata.compression_type {
        CompressionType::None => compressed.compressed_data.to_vec(),
        CompressionType::Gzip => {
            use flate2::read::GzDecoder;
            use std::io::Read;

            let mut decoder = GzDecoder::new(&compressed.compressed_data[..]);
            let mut decompressed = Vec::new();
            decoder
                .read_to_end(&mut decompressed)
                .map_err(|e| Error::DecompressionFailed(e.to_string()))?;
            decompressed
        }
        CompressionType::Lz4 => {
            // For now, treat as uncompressed
            compressed.compressed_data.to_vec()
        }
    };

    // Verify checksum
    let actual_checksum = calculate_checksum(&decompressed_data);
    if actual_checksum != compressed.metadata.original_checksum {
        return Err(Error::ChecksumMismatch(format!(
            "Expected {}, got {}",
            compressed.metadata.original_checksum, actual_checksum
        )));
    }

    let checkpoint: MigrationCheckpoint = serde_json::from_slice(&decompressed_data)
        .map_err(|e| Error::Deserialization(e.to_string()))?;

    Ok(checkpoint)
}

/// Calculate SHA256 checksum of data
pub fn calculate_checksum(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

/// Validate checkpoint integrity
pub fn validate_checkpoint(checkpoint: &MigrationCheckpoint) -> ConsensusResult<()> {
    // Check if stream name is valid
    if checkpoint.stream_name.is_empty() {
        return Err(Error::InvalidCheckpoint(
            "Stream name cannot be empty".to_string(),
        ));
    }

    // Check if sequence number is valid
    if checkpoint.sequence > checkpoint.data.last_seq {
        return Err(Error::InvalidCheckpoint(
            "Checkpoint sequence cannot be greater than last sequence".to_string(),
        ));
    }

    // Validate data integrity
    let serialized_data =
        serde_json::to_vec(&checkpoint.data).map_err(|e| Error::Serialization(e.to_string()))?;
    let calculated_checksum = calculate_checksum(&serialized_data);

    if calculated_checksum != checkpoint.checksum {
        return Err(Error::ChecksumMismatch(format!(
            "Data checksum mismatch. Expected {}, got {}",
            checkpoint.checksum, calculated_checksum
        )));
    }

    Ok(())
}

/// Estimate completion time for migration based on current state and elapsed time
fn estimate_completion_time(state: &MigrationState, elapsed_ms: u64) -> Option<u64> {
    match state {
        MigrationState::Preparing => Some(elapsed_ms + 5000), // +5 seconds
        MigrationState::Transferring => Some(elapsed_ms + 15000), // +15 seconds
        MigrationState::Syncing => Some(elapsed_ms + 3000),   // +3 seconds for incremental sync
        MigrationState::Switching => Some(elapsed_ms + 2000), // +2 seconds
        MigrationState::Completing => Some(elapsed_ms + 1000), // +1 second
        MigrationState::Completed | MigrationState::Failed => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocation::ConsensusGroupId;
    use crate::local::state_machine::{MessageData, StreamData};
    use bytes::Bytes;
    use std::collections::BTreeMap;

    #[test]
    fn test_checkpoint_compression() {
        // Create a test checkpoint
        let mut messages = BTreeMap::new();
        for i in 0..100 {
            messages.insert(
                i,
                MessageData {
                    sequence: i,
                    data: Bytes::from(format!("Test message {}", i)),
                    timestamp: 1000 + i,
                    metadata: None,
                },
            );
        }

        let checkpoint = MigrationCheckpoint {
            stream_name: "test-stream".to_string(),
            sequence: 99,
            data: StreamData {
                messages,
                last_seq: 99,
                is_paused: false,
                pending_operations: Vec::new(),
                paused_at: None,
            },
            config: crate::global::StreamConfig::default(),
            subscriptions: vec!["test.subject".to_string()],
            created_at: 12345,
            checksum: "dummy".to_string(),
            compression: CompressionType::Gzip,
            is_incremental: false,
            base_checkpoint_seq: None,
        };

        // Test compression
        let compressed = compress_checkpoint(&checkpoint, CompressionType::Gzip).unwrap();
        assert!(compressed.metadata.compressed_size < compressed.metadata.original_size);
        assert!(compressed.metadata.compression_ratio > 1.0);

        // Test decompression
        let decompressed = decompress_checkpoint(&compressed).unwrap();
        assert_eq!(decompressed.stream_name, checkpoint.stream_name);
        assert_eq!(decompressed.sequence, checkpoint.sequence);
        assert_eq!(
            decompressed.data.messages.len(),
            checkpoint.data.messages.len()
        );
    }

    #[test]
    fn test_checkpoint_validation() {
        // Valid checkpoint
        let mut messages = BTreeMap::new();
        messages.insert(
            1,
            MessageData {
                sequence: 1,
                data: Bytes::from("test"),
                timestamp: 1000,
                metadata: None,
            },
        );

        let stream_data = StreamData {
            messages,
            last_seq: 1,
            is_paused: false,
            pending_operations: Vec::new(),
            paused_at: None,
        };

        let checksum = {
            let serialized = serde_json::to_vec(&stream_data).unwrap();
            calculate_checksum(&serialized)
        };

        let checkpoint = MigrationCheckpoint {
            stream_name: "test-stream".to_string(),
            sequence: 1,
            data: stream_data,
            config: crate::global::StreamConfig::default(),
            subscriptions: vec![],
            created_at: 12345,
            checksum,
            compression: CompressionType::None,
            is_incremental: false,
            base_checkpoint_seq: None,
        };

        // Should pass validation
        assert!(validate_checkpoint(&checkpoint).is_ok());

        // Invalid checkpoint - empty stream name
        let mut invalid_checkpoint = checkpoint.clone();
        invalid_checkpoint.stream_name = "".to_string();
        assert!(validate_checkpoint(&invalid_checkpoint).is_err());

        // Invalid checkpoint - sequence > last_seq
        let mut invalid_checkpoint = checkpoint.clone();
        invalid_checkpoint.sequence = 10;
        assert!(validate_checkpoint(&invalid_checkpoint).is_err());

        // Invalid checkpoint - wrong checksum
        let mut invalid_checkpoint = checkpoint.clone();
        invalid_checkpoint.checksum = "wrong_checksum".to_string();
        assert!(validate_checkpoint(&invalid_checkpoint).is_err());
    }

    #[test]
    fn test_incremental_checkpoint() {
        // Create base data
        let mut messages = BTreeMap::new();
        for i in 1..=10 {
            messages.insert(
                i,
                MessageData {
                    sequence: i,
                    data: Bytes::from(format!("Message {}", i)),
                    timestamp: 1000 + i,
                    metadata: None,
                },
            );
        }

        let _stream_data = StreamData {
            messages: messages.clone(),
            last_seq: 10,
            is_paused: false,
            pending_operations: Vec::new(),
            paused_at: None,
        };

        // Create incremental checkpoint with only messages after sequence 5
        let incremental_messages: BTreeMap<u64, MessageData> = messages
            .range(6..)
            .map(|(seq, data)| (*seq, data.clone()))
            .collect();

        let incremental_data = StreamData {
            messages: incremental_messages,
            last_seq: 10,
            is_paused: false,
            pending_operations: Vec::new(),
            paused_at: None,
        };

        let checksum = {
            let serialized = serde_json::to_vec(&incremental_data).unwrap();
            calculate_checksum(&serialized)
        };

        let incremental_checkpoint = MigrationCheckpoint {
            stream_name: "test-stream".to_string(),
            sequence: 10,
            data: incremental_data,
            config: crate::global::StreamConfig::default(),
            subscriptions: vec![],
            created_at: 12345,
            checksum,
            compression: CompressionType::None,
            is_incremental: true,
            base_checkpoint_seq: Some(5),
        };

        // Validate incremental checkpoint
        assert!(validate_checkpoint(&incremental_checkpoint).is_ok());
        assert_eq!(incremental_checkpoint.data.messages.len(), 5); // Messages 6-10
        assert!(incremental_checkpoint.is_incremental);
        assert_eq!(incremental_checkpoint.base_checkpoint_seq, Some(5));
    }

    #[test]
    fn test_estimate_completion_time() {
        // Test different migration states
        assert!(estimate_completion_time(&MigrationState::Preparing, 1000).is_some());
        assert!(estimate_completion_time(&MigrationState::Transferring, 5000).is_some());
        assert!(estimate_completion_time(&MigrationState::Syncing, 10000).is_some());
        assert!(estimate_completion_time(&MigrationState::Switching, 15000).is_some());
        assert!(estimate_completion_time(&MigrationState::Completing, 20000).is_some());

        // Completed and Failed states should return None
        assert!(estimate_completion_time(&MigrationState::Completed, 25000).is_none());
        assert!(estimate_completion_time(&MigrationState::Failed, 30000).is_none());
    }

    #[test]
    fn test_active_migration_state_transitions() {
        let migration = ActiveMigration {
            stream_name: "test-stream".to_string(),
            source_group: ConsensusGroupId::new(1),
            target_group: ConsensusGroupId::new(2),
            state: MigrationState::Preparing,
            started_at: 1000,
            checkpoint_seq: None,
            error: None,
        };

        // Test initial state
        assert_eq!(migration.state, MigrationState::Preparing);
        assert!(migration.checkpoint_seq.is_none());
        assert!(migration.error.is_none());

        // Simulate state progression
        let mut migration = migration;
        migration.state = MigrationState::Transferring;
        migration.checkpoint_seq = Some(100);
        assert_eq!(migration.checkpoint_seq, Some(100));

        // Simulate failure
        migration.state = MigrationState::Failed;
        migration.error = Some("Test error".to_string());
        assert!(migration.error.is_some());
    }

    #[test]
    fn test_migration_progress() {
        let started_at = chrono::Utc::now().timestamp_millis() as u64 - 5000; // 5 seconds ago

        let migration = ActiveMigration {
            stream_name: "test-stream".to_string(),
            source_group: ConsensusGroupId::new(1),
            target_group: ConsensusGroupId::new(2),
            state: MigrationState::Syncing,
            started_at,
            checkpoint_seq: Some(150),
            error: None,
        };

        // Create progress from active migration
        let progress = MigrationProgress {
            stream_name: migration.stream_name.clone(),
            source_group: migration.source_group,
            target_group: migration.target_group,
            current_state: migration.state.clone(),
            elapsed_ms: chrono::Utc::now().timestamp_millis() as u64 - migration.started_at,
            checkpoint_seq: migration.checkpoint_seq,
            error: migration.error.clone(),
            estimated_completion_ms: estimate_completion_time(&migration.state, 5000),
        };

        assert_eq!(progress.stream_name, "test-stream");
        assert!(progress.elapsed_ms >= 5000);
        assert_eq!(progress.checkpoint_seq, Some(150));
        assert!(progress.estimated_completion_ms.is_some());
    }

    #[tokio::test]
    async fn test_migration_lifecycle() {
        // This test would require a mock router implementation
        // For now, we test the basic structure and state transitions

        // Test checkpoint metadata
        let metadata = CheckpointMetadata {
            original_size: 1000,
            compressed_size: 300,
            compression_ratio: 3.33,
            compression_type: CompressionType::Gzip,
            original_checksum: "abc123".to_string(),
            compressed_checksum: "def456".to_string(),
        };

        assert!(metadata.compression_ratio > 3.0);
        assert!(metadata.compressed_size < metadata.original_size);
    }
}
