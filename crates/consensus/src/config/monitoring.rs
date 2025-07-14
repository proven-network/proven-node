//! Monitoring and metrics configuration
//!
//! This module provides configuration for monitoring, metrics collection,
//! and alerting within the consensus system.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration for monitoring and metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    /// Enable monitoring
    pub enabled: bool,
    /// Metrics update interval
    pub update_interval: Duration,
    /// Health check configuration
    pub health_check: HealthCheckConfig,
    /// Prometheus endpoint configuration
    pub prometheus: PrometheusConfig,
    /// Alerting configuration
    pub alerting: AlertingConfig,
    /// Stream monitoring configuration
    pub stream_monitoring: StreamMonitoringConfig,
    /// Group monitoring configuration
    pub group_monitoring: GroupMonitoringConfig,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            update_interval: Duration::from_secs(60),
            health_check: HealthCheckConfig::default(),
            prometheus: PrometheusConfig::default(),
            alerting: AlertingConfig::default(),
            stream_monitoring: StreamMonitoringConfig::default(),
            group_monitoring: GroupMonitoringConfig::default(),
        }
    }
}

/// Health check configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    /// Enable health checks
    pub enabled: bool,
    /// Health check interval
    pub interval: Duration,
    /// Timeout for health check operations
    pub timeout: Duration,
    /// Number of failures before marking unhealthy
    pub failure_threshold: u32,
    /// Number of successes before marking healthy
    pub success_threshold: u32,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(5),
            failure_threshold: 3,
            success_threshold: 2,
        }
    }
}

/// Prometheus configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrometheusConfig {
    /// Enable Prometheus metrics export
    pub enabled: bool,
    /// HTTP endpoint for metrics
    pub endpoint: String,
    /// Port for metrics server
    pub port: u16,
    /// Namespace for all metrics
    pub namespace: String,
    /// Subsystem for metrics
    pub subsystem: String,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            endpoint: "/metrics".to_string(),
            port: 9090,
            namespace: "proven".to_string(),
            subsystem: "consensus".to_string(),
        }
    }
}

/// Alerting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertingConfig {
    /// Enable alerting
    pub enabled: bool,
    /// Alert when load imbalance exceeds this ratio
    pub imbalance_threshold: f64,
    /// Alert when migration failure rate exceeds this percentage
    pub migration_failure_threshold: f64,
    /// Alert when a group is unhealthy for this duration
    pub unhealthy_duration_threshold: Duration,
    /// Alert when stream operation latency exceeds this
    pub high_latency_threshold: Duration,
    /// Alert when error rate exceeds this percentage
    pub error_rate_threshold: f64,
    /// Webhook URL for sending alerts
    pub webhook_url: Option<String>,
    /// Alert aggregation window
    pub aggregation_window: Duration,
}

impl Default for AlertingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            imbalance_threshold: 3.0,
            migration_failure_threshold: 0.2, // 20%
            unhealthy_duration_threshold: Duration::from_secs(300), // 5 minutes
            high_latency_threshold: Duration::from_secs(5),
            error_rate_threshold: 0.05, // 5%
            webhook_url: None,
            aggregation_window: Duration::from_secs(300), // 5 minutes
        }
    }
}

/// Stream monitoring configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMonitoringConfig {
    /// Enable stream-level monitoring
    pub enabled: bool,
    /// Track operation latencies
    pub track_latencies: bool,
    /// Track message rates
    pub track_message_rates: bool,
    /// Track storage usage
    pub track_storage_usage: bool,
    /// Sampling rate for detailed metrics (0.0 to 1.0)
    pub sampling_rate: f64,
    /// Maximum number of streams to track individually
    pub max_tracked_streams: usize,
    /// Enable load balance monitoring
    pub enable_load_monitoring: bool,
    /// Maximum ratio for load imbalance detection
    pub load_balance_max_ratio: f64,
    /// Enable automatic rebalancing
    pub enable_auto_rebalancing: bool,
    /// Interval between rebalancing checks
    pub rebalancing_check_interval: Duration,
    /// Minimum time between rebalancing operations
    pub rebalancing_cooldown: Duration,
}

impl Default for StreamMonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            track_latencies: true,
            track_message_rates: true,
            track_storage_usage: true,
            sampling_rate: 0.1, // 10% sampling
            max_tracked_streams: 1000,
            enable_load_monitoring: true,
            load_balance_max_ratio: 3.0, // 3x average is considered overloaded
            enable_auto_rebalancing: false, // Disabled by default
            rebalancing_check_interval: Duration::from_secs(300), // Check every 5 minutes
            rebalancing_cooldown: Duration::from_secs(900), // 15 minutes between rebalancing
        }
    }
}

/// Group monitoring configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMonitoringConfig {
    /// Enable group-level monitoring
    pub enabled: bool,
    /// Track group health status
    pub track_health: bool,
    /// Track load distribution
    pub track_load_distribution: bool,
    /// Track node membership changes
    pub track_membership: bool,
    /// Track consensus performance metrics
    pub track_consensus_metrics: bool,
    /// Interval for collecting group metrics
    pub collection_interval: Duration,
    /// Enable membership tracking for the local node
    pub enable_membership_tracking: bool,
}

impl Default for GroupMonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            track_health: true,
            track_load_distribution: true,
            track_membership: true,
            track_consensus_metrics: true,
            collection_interval: Duration::from_secs(60),
            enable_membership_tracking: true,
        }
    }
}

/// Builder for monitoring configuration
pub struct MonitoringConfigBuilder {
    config: MonitoringConfig,
}

impl MonitoringConfigBuilder {
    /// Create a new builder with default configuration
    pub fn new() -> Self {
        Self {
            config: MonitoringConfig::default(),
        }
    }

    /// Set whether monitoring is enabled
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    /// Set the update interval
    pub fn update_interval(mut self, interval: Duration) -> Self {
        self.config.update_interval = interval;
        self
    }

    /// Set health check configuration
    pub fn health_check(mut self, health_check: HealthCheckConfig) -> Self {
        self.config.health_check = health_check;
        self
    }

    /// Set Prometheus configuration
    pub fn prometheus(mut self, prometheus: PrometheusConfig) -> Self {
        self.config.prometheus = prometheus;
        self
    }

    /// Set alerting configuration
    pub fn alerting(mut self, alerting: AlertingConfig) -> Self {
        self.config.alerting = alerting;
        self
    }

    /// Set stream monitoring configuration
    pub fn stream_monitoring(mut self, stream_monitoring: StreamMonitoringConfig) -> Self {
        self.config.stream_monitoring = stream_monitoring;
        self
    }

    /// Set group monitoring configuration
    pub fn group_monitoring(mut self, group_monitoring: GroupMonitoringConfig) -> Self {
        self.config.group_monitoring = group_monitoring;
        self
    }

    /// Build the configuration
    pub fn build(self) -> MonitoringConfig {
        self.config
    }
}

impl Default for MonitoringConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = MonitoringConfig::default();

        assert!(config.enabled);
        assert_eq!(config.update_interval, Duration::from_secs(60));
        assert!(config.health_check.enabled);
        assert!(config.prometheus.enabled);
        assert!(!config.alerting.enabled);
        assert!(config.stream_monitoring.enabled);
        assert!(config.group_monitoring.enabled);
    }

    #[test]
    fn test_monitoring_builder() {
        let config = MonitoringConfigBuilder::new()
            .enabled(false)
            .update_interval(Duration::from_secs(30))
            .prometheus(PrometheusConfig {
                enabled: true,
                port: 9091,
                ..Default::default()
            })
            .alerting(AlertingConfig {
                enabled: true,
                webhook_url: Some("https://example.com/alerts".to_string()),
                ..Default::default()
            })
            .build();

        assert!(!config.enabled);
        assert_eq!(config.update_interval, Duration::from_secs(30));
        assert_eq!(config.prometheus.port, 9091);
        assert!(config.alerting.enabled);
        assert_eq!(
            config.alerting.webhook_url.as_ref().unwrap(),
            "https://example.com/alerts"
        );
    }

    #[test]
    fn test_serialization() {
        let config = MonitoringConfig::default();

        // Serialize to JSON
        let json = serde_json::to_string_pretty(&config).unwrap();

        // Deserialize back
        let deserialized: MonitoringConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.enabled, config.enabled);
        assert_eq!(deserialized.update_interval, config.update_interval);
        assert_eq!(deserialized.prometheus.port, config.prometheus.port);
    }

    #[test]
    fn test_health_check_config() {
        let health_check = HealthCheckConfig {
            enabled: true,
            interval: Duration::from_secs(20),
            timeout: Duration::from_secs(10),
            failure_threshold: 5,
            success_threshold: 3,
        };

        assert_eq!(health_check.interval, Duration::from_secs(20));
        assert_eq!(health_check.failure_threshold, 5);
        assert_eq!(health_check.success_threshold, 3);
    }

    #[test]
    fn test_stream_monitoring_config() {
        let stream_config = StreamMonitoringConfig {
            enabled: true,
            track_latencies: false,
            track_message_rates: true,
            track_storage_usage: false,
            sampling_rate: 0.25,
            max_tracked_streams: 500,
            enable_load_monitoring: true,
            load_balance_max_ratio: 3.0,
            enable_auto_rebalancing: false,
            rebalancing_check_interval: Duration::from_secs(300),
            rebalancing_cooldown: Duration::from_secs(900),
        };

        assert!(!stream_config.track_latencies);
        assert!(stream_config.track_message_rates);
        assert_eq!(stream_config.sampling_rate, 0.25);
        assert_eq!(stream_config.max_tracked_streams, 500);
    }

    #[test]
    fn test_alerting_thresholds() {
        let alerting = AlertingConfig::default();

        assert_eq!(alerting.imbalance_threshold, 3.0);
        assert_eq!(alerting.migration_failure_threshold, 0.2);
        assert_eq!(alerting.error_rate_threshold, 0.05);
        assert_eq!(
            alerting.unhealthy_duration_threshold,
            Duration::from_secs(300)
        );
    }
}
