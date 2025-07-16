//! Types for the migration service

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::foundation::ConsensusGroupId;
use proven_topology::NodeId;

/// Result type for migration operations
pub type MigrationResult<T> = Result<T, MigrationError>;

/// Errors that can occur during migration
#[derive(Debug, Error)]
pub enum MigrationError {
    /// Service not started
    #[error("Migration service not started")]
    NotStarted,

    /// Migration already in progress
    #[error("Migration already in progress for {0}")]
    AlreadyInProgress(String),

    /// Migration not found
    #[error("Migration not found: {0}")]
    NotFound(String),

    /// Invalid migration state
    #[error("Invalid migration state: {0}")]
    InvalidState(String),

    /// Source group error
    #[error("Source group error: {0}")]
    SourceError(String),

    /// Target group error
    #[error("Target group error: {0}")]
    TargetError(String),

    /// Allocation error
    #[error("Allocation error: {0}")]
    AllocationError(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Stream migration state
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StreamMigrationState {
    /// Migration is pending
    Pending,
    /// Preparing for migration
    Preparing,
    /// Exporting data from source
    Exporting { progress: f32 },
    /// Transferring data
    Transferring { progress: f32 },
    /// Importing data to target
    Importing { progress: f32 },
    /// Verifying migration
    Verifying,
    /// Migration completed
    Completed,
    /// Migration failed
    Failed { reason: String },
    /// Migration cancelled
    Cancelled,
}

/// Node migration state
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeMigrationState {
    /// Migration pending
    Pending,
    /// Node joining target group
    Joining,
    /// Syncing data in target group
    Syncing { progress: f32 },
    /// Ready to leave source group
    ReadyToLeave,
    /// Leaving source group
    Leaving,
    /// Migration completed
    Completed,
    /// Migration failed
    Failed { reason: String },
}

/// Migration progress information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationProgress {
    /// Current state
    pub state: MigrationStatus,
    /// Start time
    pub started_at: SystemTime,
    /// Last update time
    pub last_update: SystemTime,
    /// Estimated completion time
    pub estimated_completion: Option<SystemTime>,
    /// Progress percentage (0-100)
    pub progress_percent: f32,
    /// Bytes transferred
    pub bytes_transferred: u64,
    /// Total bytes to transfer
    pub total_bytes: Option<u64>,
    /// Current phase description
    pub current_phase: String,
}

/// Overall migration status
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MigrationStatus {
    /// Stream migration
    Stream(StreamMigrationState),
    /// Node migration
    Node(NodeMigrationState),
}

/// Rebalancing plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalancingPlan {
    /// Plan ID
    pub id: String,
    /// Creation time
    pub created_at: SystemTime,
    /// Stream migrations to perform
    pub stream_migrations: Vec<PlannedStreamMigration>,
    /// Node migrations to perform
    pub node_migrations: Vec<PlannedNodeMigration>,
    /// Expected improvement score
    pub improvement_score: f64,
    /// Execution strategy
    pub strategy: RebalancingStrategy,
}

/// Planned stream migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedStreamMigration {
    /// Stream name
    pub stream_name: String,
    /// Source group
    pub from_group: ConsensusGroupId,
    /// Target group
    pub to_group: ConsensusGroupId,
    /// Reason for migration
    pub reason: MigrationReason,
    /// Priority (higher = more urgent)
    pub priority: u8,
}

/// Planned node migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedNodeMigration {
    /// Node ID
    pub node_id: NodeId,
    /// Source group
    pub from_group: ConsensusGroupId,
    /// Target group
    pub to_group: ConsensusGroupId,
    /// Reason for migration
    pub reason: MigrationReason,
    /// Priority
    pub priority: u8,
}

/// Reason for migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MigrationReason {
    /// Load balancing
    LoadBalance,
    /// Node failure
    NodeFailure,
    /// Geographic optimization
    GeographicOptimization,
    /// Manual request
    Manual,
    /// Group consolidation
    Consolidation,
}

/// Rebalancing strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RebalancingStrategy {
    /// Perform all migrations at once
    Aggressive,
    /// Perform migrations gradually
    Conservative,
    /// Perform migrations based on load
    Adaptive,
}

/// Allocation strategy for groups
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum AllocationStrategy {
    /// Balance by region
    Regional,
    /// Balance by load
    LoadBased,
    /// Balance by node count
    RoundRobin,
    /// Custom strategy
    Custom,
}

/// Migration configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationConfig {
    /// Enable stream migration
    pub enable_stream_migration: bool,

    /// Enable node migration
    pub enable_node_migration: bool,

    /// Enable automatic rebalancing
    pub enable_auto_rebalancing: bool,

    /// Stream migration settings
    pub stream_migration: StreamMigrationConfig,

    /// Node migration settings
    pub node_migration: NodeMigrationConfig,

    /// Rebalancing settings
    pub rebalancing: RebalancingConfig,

    /// Allocation settings
    pub allocation: AllocationConfig,
}

/// Stream migration configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMigrationConfig {
    /// Batch size for data transfer
    pub batch_size: usize,

    /// Transfer timeout
    pub transfer_timeout: Duration,

    /// Verify after migration
    pub verify_migration: bool,

    /// Compression during transfer
    pub enable_compression: bool,

    /// Max concurrent migrations
    pub max_concurrent: usize,
}

/// Node migration configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMigrationConfig {
    /// Sync timeout
    pub sync_timeout: Duration,

    /// Grace period before removal
    pub grace_period: Duration,

    /// Max concurrent migrations
    pub max_concurrent: usize,
}

/// Rebalancing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalancingConfig {
    /// Check interval
    pub check_interval: Duration,

    /// Imbalance threshold (0.0-1.0)
    pub imbalance_threshold: f32,

    /// Min time between rebalances
    pub cooldown_period: Duration,

    /// Default strategy
    pub default_strategy: RebalancingStrategy,
}

/// Allocation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationConfig {
    /// Default strategy
    pub default_strategy: AllocationStrategy,

    /// Groups per node
    pub groups_per_node: usize,

    /// Min nodes per group
    pub min_nodes_per_group: usize,

    /// Max nodes per group
    pub max_nodes_per_group: usize,

    /// Regional preferences
    pub regional_preferences: HashMap<String, f32>,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            enable_stream_migration: true,
            enable_node_migration: true,
            enable_auto_rebalancing: false,
            stream_migration: StreamMigrationConfig::default(),
            node_migration: NodeMigrationConfig::default(),
            rebalancing: RebalancingConfig::default(),
            allocation: AllocationConfig::default(),
        }
    }
}

impl Default for StreamMigrationConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            transfer_timeout: Duration::from_secs(300), // 5 minutes
            verify_migration: true,
            enable_compression: true,
            max_concurrent: 3,
        }
    }
}

impl Default for NodeMigrationConfig {
    fn default() -> Self {
        Self {
            sync_timeout: Duration::from_secs(600), // 10 minutes
            grace_period: Duration::from_secs(30),
            max_concurrent: 2,
        }
    }
}

impl Default for RebalancingConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(300),  // 5 minutes
            imbalance_threshold: 0.2,                  // 20% imbalance
            cooldown_period: Duration::from_secs(900), // 15 minutes
            default_strategy: RebalancingStrategy::Conservative,
        }
    }
}

impl Default for AllocationConfig {
    fn default() -> Self {
        Self {
            default_strategy: AllocationStrategy::Regional,
            groups_per_node: 3,
            min_nodes_per_group: 3,
            max_nodes_per_group: 7,
            regional_preferences: HashMap::new(),
        }
    }
}
