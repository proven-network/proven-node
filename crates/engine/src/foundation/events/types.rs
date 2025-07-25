//! Common types used throughout the event system

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};
use tracing::Span;
use uuid::Uuid;

/// Unique identifier for an event or request
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventId(Uuid);

impl EventId {
    /// Create a new unique event ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

/// Priority levels for event/request processing
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    /// Processed immediately, may block other events
    Critical = 0,
    /// High priority, processed before normal
    High = 1,
    /// Default priority
    Normal = 2,
    /// Low priority, processed when system is idle
    Low = 3,
}

/// Metadata attached to every event/request
#[derive(Debug, Clone)]
pub struct EventMetadata {
    /// Unique ID for this event
    pub id: EventId,
    /// When the event was created
    pub timestamp: SystemTime,
    /// Priority for processing
    pub priority: Priority,
    /// Tracing span for distributed tracing
    pub span: Option<Span>,
    /// Source service/component
    pub source: String,
    /// Optional correlation ID for request tracking
    pub correlation_id: Option<String>,
    /// Optional deadline for processing
    pub deadline: Option<SystemTime>,
}

impl EventMetadata {
    /// Create a new event metadata with the given source
    pub fn new(source: impl Into<String>) -> Self {
        Self {
            id: EventId::new(),
            timestamp: SystemTime::now(),
            priority: Priority::Normal,
            span: None,
            source: source.into(),
            correlation_id: None,
            deadline: None,
        }
    }

    /// Set the priority for this event
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// Set the deadline for this event
    pub fn with_deadline(mut self, deadline: SystemTime) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Set the timeout for this event
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.deadline = Some(SystemTime::now() + timeout);
        self
    }

    /// Set the correlation ID for this event
    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Check if this event has expired
    pub fn is_expired(&self) -> bool {
        if let Some(deadline) = self.deadline {
            SystemTime::now() > deadline
        } else {
            false
        }
    }
}

/// Statistics for a specific event/request type
#[derive(Debug, Default)]
pub struct TypeStats {
    /// Total number of events/requests sent
    pub sent_count: AtomicU64,
    /// Total number successfully processed
    pub success_count: AtomicU64,
    /// Total number that failed
    pub error_count: AtomicU64,
    /// Total number that timed out
    pub timeout_count: AtomicU64,
    /// Total processing time in microseconds
    pub total_duration_us: AtomicU64,
    /// Number of events currently in flight
    pub in_flight: AtomicU64,
}

impl TypeStats {
    pub fn record_sent(&self) {
        self.sent_count.fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_success(&self, duration_us: u64) {
        self.success_count.fetch_add(1, Ordering::Relaxed);
        self.total_duration_us
            .fetch_add(duration_us, Ordering::Relaxed);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn record_timeout(&self) {
        self.timeout_count.fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn average_duration_us(&self) -> f64 {
        let count = self.success_count.load(Ordering::Relaxed);
        if count == 0 {
            0.0
        } else {
            self.total_duration_us.load(Ordering::Relaxed) as f64 / count as f64
        }
    }
}

/// Configuration for channel bounds and behavior
#[derive(Debug, Clone)]
pub struct ChannelConfig {
    /// Maximum number of events that can be queued
    pub capacity: Option<usize>,
    /// What to do when channel is full
    pub overflow_policy: OverflowPolicy,
    /// Enable metrics collection for this channel
    pub metrics_enabled: bool,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            capacity: Some(10_000),
            overflow_policy: OverflowPolicy::Block,
            metrics_enabled: true,
        }
    }
}

/// Policy for handling channel overflow
#[derive(Debug, Clone, Copy)]
pub enum OverflowPolicy {
    /// Block until space is available
    Block,
    /// Drop the oldest event
    DropOldest,
    /// Drop the new event
    DropNewest,
    /// Return an error immediately
    Error,
}
