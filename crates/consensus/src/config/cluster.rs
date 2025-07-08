//! Cluster configuration types

use std::time::Duration;

/// Configuration for cluster join retry behavior
#[derive(Debug, Clone)]
pub struct ClusterJoinRetryConfig {
    /// Maximum number of join attempts before giving up (default: 5)
    pub max_attempts: usize,
    /// Initial delay between retry attempts (default: 500ms)
    pub initial_delay: Duration,
    /// Maximum delay between retry attempts (default: 5s)
    pub max_delay: Duration,
    /// Timeout for each individual join request (default: 30s)
    pub request_timeout: Duration,
}

impl Default for ClusterJoinRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
        }
    }
}
