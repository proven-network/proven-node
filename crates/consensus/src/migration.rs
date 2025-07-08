//! Stream migration protocol for load balancing
//!
//! This module handles the migration of streams between consensus groups
//! to balance load and ensure optimal performance.

use crate::allocation::ConsensusGroupId;
use crate::error::{ConsensusResult, Error};
use crate::global::{GlobalOperation, StreamConfig};
use crate::local::state_machine::StreamData;
use crate::operations::{LocalStreamOperation, MigrationState};
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
        // TODO: In a real implementation, this would be a specialized operation
        // that only captures messages after since_seq
        let operation = LocalStreamOperation::GetStreamCheckpoint {
            stream_name: migration.stream_name.clone(),
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

        // For now, create a basic incremental checkpoint
        // In a real implementation, this would only include messages after since_seq
        let stream_data = StreamData {
            messages: std::collections::BTreeMap::new(), // Would contain only new messages
            last_seq: response.sequence.unwrap_or(since_seq),
            is_paused: false,
            pending_operations: Vec::new(),
            paused_at: None,
        };

        let serialized_data =
            serde_json::to_vec(&stream_data).map_err(|e| Error::Serialization(e.to_string()))?;
        let checksum = calculate_checksum(&serialized_data);

        let checkpoint = MigrationCheckpoint {
            stream_name: migration.stream_name.clone(),
            sequence: response.sequence.unwrap_or(since_seq),
            data: stream_data,
            config: crate::global::StreamConfig::default(),
            subscriptions: Vec::new(),
            created_at: chrono::Utc::now().timestamp_millis() as u64,
            checksum,
            compression: CompressionType::default(),
            is_incremental: true,
            base_checkpoint_seq: Some(since_seq),
        };

        validate_checkpoint(&checkpoint)?;
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

        // TODO: Implement specialized incremental apply operation
        let operation = LocalStreamOperation::ApplyMigrationCheckpoint {
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

        // TODO: In a real implementation, we would retrieve the actual checkpoint
        // from the response or from a temporary storage location.
        // For now, create a basic checkpoint structure
        let stream_data = StreamData {
            messages: std::collections::BTreeMap::new(),
            last_seq: response.sequence.unwrap_or(0),
            is_paused: false,
            pending_operations: Vec::new(),
            paused_at: None,
        };

        let serialized_data =
            serde_json::to_vec(&stream_data).map_err(|e| Error::Serialization(e.to_string()))?;
        let checksum = calculate_checksum(&serialized_data);

        let checkpoint = MigrationCheckpoint {
            stream_name: migration.stream_name.clone(),
            sequence: response.sequence.unwrap_or(0),
            data: stream_data,
            config: crate::global::StreamConfig::default(),
            subscriptions: Vec::new(),
            created_at: chrono::Utc::now().timestamp_millis() as u64,
            checksum,
            compression: CompressionType::default(),
            is_incremental: false,
            base_checkpoint_seq: None,
        };

        // Validate the checkpoint before returning
        validate_checkpoint(&checkpoint)?;

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

        // TODO: Add more validation:
        // - Verify source group exists and contains the stream
        // - Verify target group exists and has capacity
        // - Check that groups are healthy and have quorum
        // - Validate stream is not currently being accessed by critical operations

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
    #[tokio::test]
    async fn test_migration_lifecycle() {
        // Test would require a mock router implementation
        // Placeholder for future tests
    }
}
