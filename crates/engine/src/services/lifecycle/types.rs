//! Types for the lifecycle service

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use proven_topology::NodeId;

/// Result type for lifecycle operations
pub type LifecycleResult<T> = Result<T, LifecycleError>;

/// Errors that can occur during lifecycle management
#[derive(Debug, Error)]
pub enum LifecycleError {
    /// Service not started
    #[error("Lifecycle service not started")]
    NotStarted,

    /// Component already running
    #[error("Component already running: {0}")]
    AlreadyRunning(String),

    /// Component not found
    #[error("Component not found: {0}")]
    ComponentNotFound(String),

    /// Startup failed
    #[error("Startup failed: {0}")]
    StartupFailed(String),

    /// Shutdown failed
    #[error("Shutdown failed: {0}")]
    ShutdownFailed(String),

    /// Initialization failed
    #[error("Initialization failed: {0}")]
    InitializationFailed(String),

    /// Health check failed
    #[error("Health check failed: {0}")]
    HealthCheckFailed(String),

    /// Timeout error
    #[error("Operation timed out after {0} seconds")]
    Timeout(u64),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Component state
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ComponentState {
    /// Not yet initialized
    NotInitialized,
    /// Initializing
    Initializing,
    /// Initialized but not started
    Initialized,
    /// Starting up
    Starting,
    /// Running normally
    Running,
    /// Shutting down
    ShuttingDown,
    /// Stopped
    Stopped,
    /// Failed state
    Failed,
}

/// Health status of a component
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Component is healthy
    Healthy,
    /// Component is degraded but operational
    Degraded { reason: String },
    /// Component is unhealthy
    Unhealthy { reason: String },
    /// Component health is unknown
    Unknown,
}

/// Component health information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    /// Component name
    pub name: String,
    /// Current state
    pub state: ComponentState,
    /// Health status
    pub status: HealthStatus,
    /// Last health check time
    pub last_check: SystemTime,
    /// Uptime duration
    pub uptime: Option<Duration>,
    /// Error count
    pub error_count: u64,
    /// Additional metrics
    pub metrics: HashMap<String, f64>,
}

/// Startup phase
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum StartupPhase {
    /// Pre-initialization checks
    PreInit,
    /// Network initialization
    NetworkInit,
    /// Storage initialization
    StorageInit,
    /// Consensus initialization
    ConsensusInit,
    /// Service registration
    ServiceRegistration,
    /// Final startup
    FinalStartup,
    /// Completed
    Completed,
}

/// Shutdown phase
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ShutdownPhase {
    /// Pre-shutdown preparation
    PreShutdown,
    /// Stop accepting new requests
    StopAccepting,
    /// Drain in-flight requests
    DrainRequests,
    /// Stop consensus
    StopConsensus,
    /// Close connections
    CloseConnections,
    /// Cleanup resources
    Cleanup,
    /// Completed
    Completed,
}

/// Initialization mode
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum InitializationMode {
    /// Fresh start (no existing state)
    Fresh,
    /// Recovery from existing state
    Recovery,
    /// Join existing cluster
    Join,
}

/// Cluster formation strategy
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ClusterFormationStrategy {
    /// Single node cluster
    SingleNode,
    /// Multi-node with expected peers
    MultiNode { expected_peers: Vec<NodeId> },
    /// Join existing cluster
    JoinExisting { target_node: NodeId },
    /// Automatic discovery
    AutoDiscovery { timeout: Duration },
}

/// Lifecycle configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleConfig {
    /// Enable health monitoring
    pub enable_health_monitoring: bool,

    /// Health check interval
    pub health_check_interval: Duration,

    /// Startup timeout
    pub startup_timeout: Duration,

    /// Shutdown timeout
    pub shutdown_timeout: Duration,

    /// Component startup order
    pub startup_order: Vec<String>,

    /// Component shutdown order (reverse of startup if empty)
    pub shutdown_order: Vec<String>,

    /// Retry configuration
    pub retry_config: RetryConfig,

    /// Enable graceful degradation
    pub enable_graceful_degradation: bool,
}

/// Retry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Max retry attempts
    pub max_attempts: u32,

    /// Initial retry delay
    pub initial_delay: Duration,

    /// Max retry delay
    pub max_delay: Duration,

    /// Exponential backoff factor
    pub backoff_factor: f32,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            enable_health_monitoring: true,
            health_check_interval: Duration::from_secs(30),
            startup_timeout: Duration::from_secs(60),
            shutdown_timeout: Duration::from_secs(30),
            startup_order: vec![
                "network".to_string(),
                "storage".to_string(),
                "consensus".to_string(),
                "services".to_string(),
            ],
            shutdown_order: vec![], // Will use reverse of startup_order
            retry_config: RetryConfig::default(),
            enable_graceful_degradation: true,
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            backoff_factor: 2.0,
        }
    }
}

impl Default for HealthStatus {
    fn default() -> Self {
        Self::Unknown
    }
}
