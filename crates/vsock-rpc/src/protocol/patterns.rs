//! Communication patterns and request options.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Different communication patterns supported by the RPC framework.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessagePattern {
    /// Single request, single response.
    RequestResponse {
        /// Timeout for the entire operation.
        timeout: Duration,
        /// Retry policy for failed requests.
        retry_policy: RetryPolicy,
    },

    /// Single request, stream of responses.
    RequestStream {
        /// Buffer size for the response stream.
        buffer_size: usize,
        /// Timeout for initial response.
        initial_timeout: Duration,
        /// Timeout for subsequent responses.
        idle_timeout: Duration,
    },

    /// Stream of requests, single response.
    StreamRequest {
        /// Buffer size for the request stream.
        buffer_size: usize,
        /// How to aggregate multiple requests.
        aggregation: AggregationPolicy,
        /// Timeout for the final response.
        timeout: Duration,
    },

    /// Bidirectional streaming.
    BidiStream {
        /// Buffer size for both streams.
        buffer_size: usize,
        /// Idle timeout before closing streams.
        idle_timeout: Duration,
    },

    /// Fire and forget - no response expected.
    OneWay {
        /// Reliability level for delivery.
        reliability: ReliabilityLevel,
        /// Whether to wait for acknowledgment.
        wait_for_ack: bool,
    },
}

/// Retry policy for failed requests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts.
    pub max_attempts: u32,
    /// Initial delay before first retry.
    pub initial_delay: Duration,
    /// Maximum delay between retries.
    pub max_delay: Duration,
    /// Backoff multiplier.
    pub multiplier: f64,
    /// Whether to add jitter to delays.
    pub jitter: bool,
}

impl RetryPolicy {
    /// Create a new exponential backoff retry policy.
    #[must_use]
    pub const fn exponential_backoff(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: true,
        }
    }

    /// Create a fixed delay retry policy.
    #[must_use]
    pub const fn fixed_delay(max_attempts: u32, delay: Duration) -> Self {
        Self {
            max_attempts,
            initial_delay: delay,
            max_delay: delay,
            multiplier: 1.0,
            jitter: false,
        }
    }

    /// No retries.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            max_attempts: 0,
            initial_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            multiplier: 1.0,
            jitter: false,
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::exponential_backoff(3)
    }
}

/// How to aggregate multiple requests in stream patterns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregationPolicy {
    /// Process all requests and return combined result.
    All,
    /// Return result from first request, cancel others.
    First,
    /// Return result from last request.
    Last,
    /// Custom aggregation with a time window.
    Windowed(Duration),
}

/// Reliability level for one-way messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReliabilityLevel {
    /// Best effort - may be dropped.
    BestEffort,
    /// At least once delivery.
    AtLeastOnce,
    /// Exactly once delivery (requires deduplication).
    ExactlyOnce,
}

/// Options for request operations.
#[derive(Debug, Clone)]
pub struct RequestOptions {
    /// Overall timeout for the operation.
    pub timeout: Duration,
    /// Retry policy.
    pub retry_policy: RetryPolicy,
    /// Priority level (higher is more important).
    pub priority: u8,
    /// Whether to compress the payload.
    pub compress: bool,
    /// Custom headers/metadata.
    pub metadata: Vec<(String, Vec<u8>)>,
}

impl Default for RequestOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            retry_policy: RetryPolicy::default(),
            priority: 0,
            compress: false,
            metadata: Vec::new(),
        }
    }
}

impl RequestOptions {
    /// Create options with a specific timeout.
    #[must_use]
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            timeout,
            ..Default::default()
        }
    }

    /// Create options with no retries.
    #[must_use]
    pub fn no_retry() -> Self {
        Self {
            retry_policy: RetryPolicy::none(),
            ..Default::default()
        }
    }

    /// Set the priority level.
    #[must_use]
    pub const fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Enable compression.
    #[must_use]
    pub const fn compressed(mut self) -> Self {
        self.compress = true;
        self
    }

    /// Add metadata.
    #[must_use]
    pub fn with_metadata(mut self, key: String, value: Vec<u8>) -> Self {
        self.metadata.push((key, value));
        self
    }
}
