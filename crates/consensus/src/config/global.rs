//! Configuration for the global consensus layer
//!
//! This module provides configuration options for the global consensus layer,
//! including Raft settings, capacity limits, and health monitoring.

use openraft::Config as RaftConfig;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration for the global consensus layer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConsensusConfig {
    /// Raft configuration for global consensus
    pub raft_config: RaftConfig,
    /// Maximum number of streams the system can handle
    pub max_streams: usize,
    /// Maximum number of local consensus groups
    pub max_groups: u32,
    /// Interval for health checks on local groups
    pub health_check_interval: Duration,
}

impl Default for GlobalConsensusConfig {
    fn default() -> Self {
        Self {
            raft_config: RaftConfig::default(),
            max_streams: 100_000,
            max_groups: 100,
            health_check_interval: Duration::from_secs(30),
        }
    }
}

/// Builder for GlobalConsensusConfig
pub struct GlobalConsensusConfigBuilder {
    config: GlobalConsensusConfig,
}

impl GlobalConsensusConfigBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            config: GlobalConsensusConfig::default(),
        }
    }

    /// Set the Raft configuration
    pub fn raft_config(mut self, raft_config: RaftConfig) -> Self {
        self.config.raft_config = raft_config;
        self
    }

    /// Set the maximum number of streams
    pub fn max_streams(mut self, max_streams: usize) -> Self {
        self.config.max_streams = max_streams;
        self
    }

    /// Set the maximum number of groups
    pub fn max_groups(mut self, max_groups: u32) -> Self {
        self.config.max_groups = max_groups;
        self
    }

    /// Set the health check interval
    pub fn health_check_interval(mut self, interval: Duration) -> Self {
        self.config.health_check_interval = interval;
        self
    }

    /// Build the configuration
    pub fn build(self) -> GlobalConsensusConfig {
        self.config
    }
}

impl Default for GlobalConsensusConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = GlobalConsensusConfig::default();
        assert_eq!(config.max_streams, 100_000);
        assert_eq!(config.max_groups, 100);
        assert_eq!(config.health_check_interval, Duration::from_secs(30));
    }

    #[test]
    fn test_builder() {
        let config = GlobalConsensusConfigBuilder::new()
            .max_streams(50_000)
            .max_groups(50)
            .health_check_interval(Duration::from_secs(60))
            .build();

        assert_eq!(config.max_streams, 50_000);
        assert_eq!(config.max_groups, 50);
        assert_eq!(config.health_check_interval, Duration::from_secs(60));
    }

    #[test]
    fn test_serialization() {
        let config = GlobalConsensusConfig::default();

        // Serialize to JSON
        let json = serde_json::to_string_pretty(&config).unwrap();

        // Deserialize back
        let deserialized: GlobalConsensusConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.max_streams, config.max_streams);
        assert_eq!(deserialized.max_groups, config.max_groups);
        assert_eq!(
            deserialized.health_check_interval,
            config.health_check_interval
        );
    }
}
