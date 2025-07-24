//! Configuration for global consensus service

use serde::{Deserialize, Serialize};
use std::time::Duration;

use proven_storage::LogIndex;

/// Configuration for global consensus service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConsensusConfig {
    /// Election timeout minimum
    pub election_timeout_min: Duration,
    /// Election timeout maximum  
    pub election_timeout_max: Duration,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Maximum entries per append
    pub max_entries_per_append: usize,
    /// Snapshot interval
    pub snapshot_interval: usize,
}

impl Default for GlobalConsensusConfig {
    fn default() -> Self {
        Self {
            // Production defaults based on typical cloud/WAN deployment
            // Heartbeat: 150ms (tolerant of network jitter)
            // Election: 1-2 seconds (prevents spurious elections)
            election_timeout_min: Duration::from_millis(1000),
            election_timeout_max: Duration::from_millis(2000),
            heartbeat_interval: Duration::from_millis(150),
            max_entries_per_append: 64,
            snapshot_interval: 10000,
        }
    }
}

/// Service state
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ServiceState {
    /// Not initialized
    NotInitialized,
    /// Initializing
    Initializing,
    /// Running
    Running,
    /// Stopping
    Stopping,
    /// Stopped
    Stopped,
}
