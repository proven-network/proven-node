//! Stream migration coordination for consensus groups
//!
//! This module handles the migration of streams between consensus groups
//! to balance load and ensure optimal performance.

use crate::ConsensusGroupId;
use crate::core::global::StreamConfig;
use crate::core::group::MigrationState;
use crate::error::{ConsensusResult, Error, MigrationError};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Configuration for stream migration
#[derive(Debug, Clone)]
pub struct StreamMigrationConfig {
    /// Maximum time allowed for a migration
    pub timeout: Duration,
    /// Batch size for transferring messages during migration
    pub batch_size: usize,
    /// Maximum concurrent migrations
    pub max_concurrent_migrations: usize,
    /// Retry configuration
    pub retry_attempts: usize,
    /// Retry delay
    pub retry_delay: Duration,
    /// Enable compression for checkpoints
    pub enable_compression: bool,
    /// Maximum checkpoint size (bytes)
    pub max_checkpoint_size: u64,
}

impl Default for StreamMigrationConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(300), // 5 minutes
            batch_size: 1000,
            max_concurrent_migrations: 3,
            retry_attempts: 3,
            retry_delay: Duration::from_secs(5),
            enable_compression: true,
            max_checkpoint_size: 100 * 1024 * 1024, // 100MB
        }
    }
}

/// Stream migration coordinator
pub struct StreamMigrationCoordinator {
    /// Configuration
    config: StreamMigrationConfig,
    /// Active migrations
    active_migrations: Arc<RwLock<HashMap<String, ActiveStreamMigration>>>,
}

/// Represents an active stream migration
#[derive(Debug, Clone)]
pub struct ActiveStreamMigration {
    /// Stream being migrated
    pub stream_name: String,
    /// Source consensus group
    pub source_group: ConsensusGroupId,
    /// Target consensus group
    pub target_group: ConsensusGroupId,
    /// Current state of migration
    pub state: StreamMigrationState,
    /// Timestamp when migration started
    pub started_at: std::time::Instant,
    /// Last checkpoint sequence number
    pub checkpoint_seq: Option<u64>,
    /// Error if migration failed
    pub error: Option<StreamMigrationError>,
    /// Progress tracking
    pub progress: StreamMigrationProgress,
}

/// State of stream migration (alias for the core MigrationState)
pub type StreamMigrationState = MigrationState;

/// Migration progress information
#[derive(Debug, Clone, Default)]
pub struct StreamMigrationProgress {
    /// Messages transferred
    pub messages_transferred: u64,
    /// Bytes transferred
    pub bytes_transferred: u64,
    /// Estimated completion percentage
    pub completion_percentage: f32,
    /// Current phase
    pub current_phase: String,
}

/// Stream migration error
#[derive(Debug, Clone)]
pub enum StreamMigrationError {
    /// Timeout exceeded
    Timeout,
    /// Checkpoint creation failed
    CheckpointFailed(String),
    /// Data transfer failed
    TransferFailed(String),
    /// Sync failed
    SyncFailed(String),
    /// Switch failed
    SwitchFailed(String),
    /// Validation failed
    ValidationFailed(String),
    /// Rollback failed
    RollbackFailed(String),
}

impl std::fmt::Display for StreamMigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => write!(f, "Migration timeout exceeded"),
            Self::CheckpointFailed(msg) => write!(f, "Checkpoint creation failed: {msg}"),
            Self::TransferFailed(msg) => write!(f, "Data transfer failed: {msg}"),
            Self::SyncFailed(msg) => write!(f, "Sync failed: {msg}"),
            Self::SwitchFailed(msg) => write!(f, "Traffic switch failed: {msg}"),
            Self::ValidationFailed(msg) => write!(f, "Validation failed: {msg}"),
            Self::RollbackFailed(msg) => write!(f, "Rollback failed: {msg}"),
        }
    }
}

/// Migration checkpoint metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationCheckpoint {
    /// Stream name
    pub stream_name: String,
    /// Sequence number up to which data has been migrated
    pub sequence: u64,
    /// Storage type
    pub storage_type: StorageType,
    /// Stream configuration
    pub config: StreamConfig,
    /// Checkpoint creation timestamp
    pub created_at: u64,
    /// Checksum for data integrity
    pub checksum: String,
    /// Compression type
    pub compression: CompressionType,
    /// Whether this is incremental
    pub is_incremental: bool,
    /// Base checkpoint sequence for incremental
    pub base_checkpoint_seq: Option<u64>,
    /// Number of messages
    pub message_count: u64,
    /// Total size in bytes
    pub total_bytes: u64,
    /// Stream metadata
    pub stream_metadata: super::types::MigrationStreamMetadata,
}

/// Checkpoint data with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointData {
    /// Checkpoint metadata
    pub metadata: CheckpointMetadata,
    /// Actual checkpoint data (compressed if applicable)
    pub data: Bytes,
}

/// Metadata for checkpoints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    /// Original size before compression
    pub original_size: u64,
    /// Compressed size
    pub compressed_size: u64,
    /// Compression type used
    pub compression_type: CompressionType,
    /// Checksum of original data
    pub original_checksum: String,
    /// Checksum of compressed data
    pub compressed_checksum: String,
}

/// Storage type for streams
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum StorageType {
    /// In-memory storage
    Memory,
    /// RocksDB storage
    RocksDB,
    /// S3 storage
    S3,
}

/// Compression type for checkpoint data
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub enum CompressionType {
    /// No compression
    #[default]
    None,
    /// Gzip compression
    Gzip,
    /// LZ4 compression
    Lz4,
}

impl StreamMigrationCoordinator {
    /// Create a new stream migration coordinator
    pub fn new(config: StreamMigrationConfig) -> Self {
        Self {
            config,
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

        // Validate preconditions
        self.validate_migration(&stream_name, source_group, target_group)
            .await?;

        // Create migration record
        let migration = ActiveStreamMigration {
            stream_name: stream_name.clone(),
            source_group,
            target_group,
            state: StreamMigrationState::Preparing,
            started_at: std::time::Instant::now(),
            checkpoint_seq: None,
            error: None,
            progress: StreamMigrationProgress::default(),
        };

        // Add to active migrations
        {
            let mut migrations = self.active_migrations.write().await;
            if migrations.contains_key(&stream_name) {
                return Err(Error::already_exists(format!(
                    "Migration for stream {stream_name} already in progress"
                )));
            }
            migrations.insert(stream_name.clone(), migration);
        }

        Ok(())
    }

    /// Execute the next step of a migration
    pub async fn execute_migration_step(&self, stream_name: &str) -> ConsensusResult<()> {
        let migration = {
            let migrations = self.active_migrations.read().await;
            migrations.get(stream_name).cloned().ok_or_else(|| {
                Error::not_found(format!("Migration for stream {stream_name} not found"))
            })?
        };

        // Check timeout
        if migration.started_at.elapsed() > self.config.timeout {
            self.fail_migration(stream_name, StreamMigrationError::Timeout)
                .await?;
            return Err(Error::MigrationFailed(MigrationError::Timeout {
                stream: stream_name.to_string(),
                seconds: self.config.timeout.as_secs(),
            }));
        }

        match migration.state {
            StreamMigrationState::Preparing => {
                self.prepare_migration(&migration).await?;
                self.create_checkpoint(&migration).await?;
                self.update_state(stream_name, StreamMigrationState::Transferring)
                    .await?;
            }
            StreamMigrationState::Transferring => {
                self.transfer_data(&migration).await?;
                self.update_state(stream_name, StreamMigrationState::Syncing)
                    .await?;
            }
            StreamMigrationState::Syncing => {
                self.sync_incremental(&migration).await?;
                self.update_state(stream_name, StreamMigrationState::Switching)
                    .await?;
            }
            StreamMigrationState::Switching => {
                self.switch_traffic(&migration).await?;
                self.update_state(stream_name, StreamMigrationState::Completing)
                    .await?;
            }
            StreamMigrationState::Completing => {
                self.complete_migration(&migration).await?;
                self.update_state(stream_name, StreamMigrationState::Completed)
                    .await?;
            }
            StreamMigrationState::Completed => {
                // Remove from active migrations
                let mut migrations = self.active_migrations.write().await;
                migrations.remove(stream_name);
                info!("Migration of stream {} completed successfully", stream_name);
            }
            StreamMigrationState::Failed => {
                warn!("Migration of stream {} is in failed state", stream_name);
                return Err(Error::MigrationFailed(MigrationError::InvalidState {
                    details: format!("Migration failed: {:?}", migration.error),
                }));
            }
        }

        Ok(())
    }

    /// Get migration progress
    pub async fn get_progress(&self, stream_name: &str) -> Option<StreamMigrationProgress> {
        let migrations = self.active_migrations.read().await;
        migrations.get(stream_name).map(|m| m.progress.clone())
    }

    /// Get all active migrations
    pub async fn get_active_migrations(&self) -> Vec<ActiveStreamMigration> {
        let migrations = self.active_migrations.read().await;
        migrations.values().cloned().collect()
    }

    /// Cancel a migration
    pub async fn cancel_migration(&self, stream_name: &str) -> ConsensusResult<()> {
        let mut migrations = self.active_migrations.write().await;

        if let Some(migration) = migrations.get_mut(stream_name) {
            if matches!(
                migration.state,
                StreamMigrationState::Completed | StreamMigrationState::Failed
            ) {
                return Err(Error::InvalidState(format!(
                    "Cannot cancel migration in {:?} state",
                    migration.state
                )));
            }

            migration.state = StreamMigrationState::Failed;
            migration.error = Some(StreamMigrationError::ValidationFailed(
                "Migration cancelled by user".to_string(),
            ));

            // Attempt rollback
            self.rollback_migration(migration).await?;
        }

        Ok(())
    }

    /// Validate migration preconditions
    async fn validate_migration(
        &self,
        _stream_name: &str,
        source_group: ConsensusGroupId,
        target_group: ConsensusGroupId,
    ) -> ConsensusResult<()> {
        // Validate groups are different
        if source_group == target_group {
            return Err(Error::InvalidOperation(
                "Source and target groups must be different".to_string(),
            ));
        }

        // Additional validation would check:
        // - Stream exists in source group
        // - Target group has capacity
        // - Both groups are healthy
        // - No ongoing migration for this stream

        Ok(())
    }

    /// Prepare target group for migration
    async fn prepare_migration(&self, migration: &ActiveStreamMigration) -> ConsensusResult<()> {
        debug!(
            "Preparing target group {:?} for stream {}",
            migration.target_group, migration.stream_name
        );

        // Create stream placeholder in target group
        // This would use the groups manager to prepare the target

        Ok(())
    }

    /// Create checkpoint of stream data
    async fn create_checkpoint(&self, migration: &ActiveStreamMigration) -> ConsensusResult<()> {
        info!(
            "Creating checkpoint for stream {} in group {:?}",
            migration.stream_name, migration.source_group
        );

        // This would interact with the groups manager to create a checkpoint
        // For now, we'll update the migration record

        let mut migrations = self.active_migrations.write().await;
        if let Some(active) = migrations.get_mut(&migration.stream_name) {
            active.checkpoint_seq = Some(100); // Example sequence
            active.progress.current_phase = "Checkpoint created".to_string();
        }

        Ok(())
    }

    /// Transfer data from source to target
    async fn transfer_data(&self, migration: &ActiveStreamMigration) -> ConsensusResult<()> {
        info!(
            "Transferring data for stream {} from {:?} to {:?}",
            migration.stream_name, migration.source_group, migration.target_group
        );

        // This would stream data from source to target group
        // Update progress as we go

        let mut migrations = self.active_migrations.write().await;
        if let Some(active) = migrations.get_mut(&migration.stream_name) {
            active.progress.messages_transferred = 1000;
            active.progress.bytes_transferred = 100_000;
            active.progress.completion_percentage = 50.0;
            active.progress.current_phase = "Data transfer in progress".to_string();
        }

        Ok(())
    }

    /// Sync incremental changes
    async fn sync_incremental(&self, migration: &ActiveStreamMigration) -> ConsensusResult<()> {
        info!(
            "Syncing incremental changes for stream {}",
            migration.stream_name
        );

        // Sync any new data that arrived during transfer

        let mut migrations = self.active_migrations.write().await;
        if let Some(active) = migrations.get_mut(&migration.stream_name) {
            active.progress.completion_percentage = 90.0;
            active.progress.current_phase = "Incremental sync".to_string();
        }

        Ok(())
    }

    /// Switch traffic to target group
    async fn switch_traffic(&self, migration: &ActiveStreamMigration) -> ConsensusResult<()> {
        info!(
            "Switching traffic for stream {} to group {:?}",
            migration.stream_name, migration.target_group
        );

        // Update stream allocation in global state
        // Pause source, resume target

        Ok(())
    }

    /// Complete the migration
    async fn complete_migration(&self, migration: &ActiveStreamMigration) -> ConsensusResult<()> {
        debug!("Completing migration for stream {}", migration.stream_name);

        // Clean up source group
        // Finalize target group

        let mut migrations = self.active_migrations.write().await;
        if let Some(active) = migrations.get_mut(&migration.stream_name) {
            active.progress.completion_percentage = 100.0;
            active.progress.current_phase = "Completed".to_string();
        }

        Ok(())
    }

    /// Update migration state
    async fn update_state(
        &self,
        stream_name: &str,
        new_state: StreamMigrationState,
    ) -> ConsensusResult<()> {
        let mut migrations = self.active_migrations.write().await;
        if let Some(migration) = migrations.get_mut(stream_name) {
            migration.state = new_state;
        }
        Ok(())
    }

    /// Fail a migration with error
    async fn fail_migration(
        &self,
        stream_name: &str,
        error: StreamMigrationError,
    ) -> ConsensusResult<()> {
        error!("Migration failed for stream {}: {:?}", stream_name, error);

        let mut migrations = self.active_migrations.write().await;
        if let Some(migration) = migrations.get_mut(stream_name) {
            migration.state = StreamMigrationState::Failed;
            migration.error = Some(error);
        }

        Ok(())
    }

    /// Rollback a failed migration
    async fn rollback_migration(&self, migration: &ActiveStreamMigration) -> ConsensusResult<()> {
        info!(
            "Rolling back migration for stream {}",
            migration.stream_name
        );

        // Restore stream to source group
        // Clean up target group
        // Update global state

        Ok(())
    }
}

/// Helper functions for checkpoint handling
impl MigrationCheckpoint {
    /// Create a new checkpoint
    pub fn new(stream_name: String, sequence: u64, config: StreamConfig) -> Self {
        Self {
            stream_name,
            sequence,
            storage_type: StorageType::Memory,
            config,
            created_at: chrono::Utc::now().timestamp_millis() as u64,
            checksum: String::new(),
            compression: CompressionType::None,
            is_incremental: false,
            base_checkpoint_seq: None,
            message_count: 0,
            total_bytes: 0,
            stream_metadata: super::types::MigrationStreamMetadata {
                is_paused: false,
                paused_at: None,
                has_pending_operations: false,
            },
        }
    }

    /// Create an incremental checkpoint
    pub fn new_incremental(
        stream_name: String,
        sequence: u64,
        base_seq: u64,
        config: StreamConfig,
    ) -> Self {
        Self {
            stream_name,
            sequence,
            storage_type: StorageType::Memory,
            config,
            created_at: chrono::Utc::now().timestamp_millis() as u64,
            checksum: String::new(),
            compression: CompressionType::None,
            is_incremental: true,
            base_checkpoint_seq: Some(base_seq),
            message_count: 0,
            total_bytes: 0,
            stream_metadata: super::types::MigrationStreamMetadata {
                is_paused: false,
                paused_at: None,
                has_pending_operations: false,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_stream_migration_lifecycle() {
        let config = StreamMigrationConfig::default();
        let coordinator = StreamMigrationCoordinator::new(config);

        // Start a migration
        let stream_name = "test-stream".to_string();
        let source_group = ConsensusGroupId::new(1);
        let target_group = ConsensusGroupId::new(2);

        coordinator
            .start_migration(stream_name.clone(), source_group, target_group)
            .await
            .unwrap();

        // Check active migrations
        let active = coordinator.get_active_migrations().await;
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].stream_name, stream_name);
        assert_eq!(active[0].state, StreamMigrationState::Preparing);

        // Check progress
        let progress = coordinator.get_progress(&stream_name).await.unwrap();
        assert_eq!(progress.completion_percentage, 0.0);
    }

    #[test]
    fn test_checkpoint_creation() {
        let checkpoint =
            MigrationCheckpoint::new("test-stream".to_string(), 100, StreamConfig::default());

        assert_eq!(checkpoint.stream_name, "test-stream");
        assert_eq!(checkpoint.sequence, 100);
        assert!(!checkpoint.is_incremental);
        assert!(checkpoint.base_checkpoint_seq.is_none());

        // Test incremental checkpoint
        let incremental = MigrationCheckpoint::new_incremental(
            "test-stream".to_string(),
            150,
            100,
            StreamConfig::default(),
        );

        assert!(incremental.is_incremental);
        assert_eq!(incremental.base_checkpoint_seq, Some(100));
    }

    #[test]
    fn test_migration_error_display() {
        let error = StreamMigrationError::Timeout;
        assert_eq!(error.to_string(), "Migration timeout exceeded");

        let error = StreamMigrationError::CheckpointFailed("test error".to_string());
        assert_eq!(error.to_string(), "Checkpoint creation failed: test error");
    }
}
