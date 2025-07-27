//! Health status types

use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// Health status levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// System is healthy
    Healthy,
    /// System is degraded but operational
    Degraded,
    /// System is unhealthy
    Unhealthy,
    /// Health status unknown
    Unknown,
}

/// Health information for a component
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    /// Component name
    pub name: String,
    /// Current status
    pub status: HealthStatus,
    /// Last check time
    pub last_check: SystemTime,
    /// Optional details
    pub details: Option<String>,
}

/// Stream health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamHealth {
    /// Stream is healthy
    Healthy,
    /// Stream is degraded (some replicas offline)
    Degraded,
    /// Stream is offline
    Offline,
}
