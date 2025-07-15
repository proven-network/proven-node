//! Configuration for group consensus service

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration for group consensus service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupConsensusConfig {
    /// Election timeout minimum
    pub election_timeout_min: Duration,
    /// Election timeout maximum  
    pub election_timeout_max: Duration,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Maximum entries per append
    pub max_entries_per_append: u64,
    /// Snapshot interval
    pub snapshot_interval: u64,
}

impl Default for GroupConsensusConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
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
