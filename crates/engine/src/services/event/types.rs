//! Types for the event service

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::foundation::ConsensusGroupId;
use crate::services::migration::MigrationStatus;
use crate::stream::StreamConfig;
use crate::stream::StreamName;
use proven_topology::NodeId;

/// Result type for event operations
pub type EventingResult<T> = Result<T, EventError>;

/// Event ID type
pub type EventId = Uuid;

/// Event timestamp type
pub type EventTimestamp = SystemTime;

/// Errors that can occur in the event service
#[derive(Debug, Error)]
pub enum EventError {
    /// Service not started
    #[error("Event service not started")]
    NotStarted,

    /// Failed to publish event
    #[error("Failed to publish event: {0}")]
    PublishFailed(String),

    /// Failed to subscribe
    #[error("Failed to subscribe: {0}")]
    SubscribeFailed(String),

    /// Event handler error
    #[error("Event handler error: {0}")]
    HandlerError(String),

    /// Event store error
    #[error("Event store error: {0}")]
    StoreError(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Timeout error
    #[error("Operation timed out")]
    Timeout,

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Events that can be emitted by consensus components
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    /// A stream was created
    StreamCreated {
        /// Stream name
        name: StreamName,
        /// Stream configuration
        config: StreamConfig,
        /// Target consensus group
        group_id: ConsensusGroupId,
    },

    /// A stream was deleted
    StreamDeleted {
        /// Stream name
        name: StreamName,
        /// Consensus group it was in
        group_id: ConsensusGroupId,
    },

    /// Stream configuration was updated
    StreamConfigUpdated {
        /// Stream name
        name: StreamName,
        /// New configuration
        config: StreamConfig,
        /// Consensus group
        group_id: ConsensusGroupId,
    },

    /// Stream is migrating between groups
    StreamMigrating {
        /// Stream name
        name: StreamName,
        /// Source group
        from_group: ConsensusGroupId,
        /// Target group
        to_group: ConsensusGroupId,
        /// Migration state
        state: MigrationStatus,
    },

    /// Stream was reallocated to a new group
    StreamReallocated {
        /// Stream name
        name: StreamName,
        /// Previous group (if any)
        old_group: Option<ConsensusGroupId>,
        /// New group
        new_group: ConsensusGroupId,
    },

    /// Message was appended to a stream (high priority)
    StreamMessageAppended {
        /// Stream name
        stream: StreamName,
        /// Consensus group ID
        group_id: ConsensusGroupId,
        /// Assigned sequence number
        sequence: u64,
        /// Message data
        message: crate::stream::MessageData,
        /// Timestamp when appended
        timestamp: u64,
        /// Consensus term when appended
        term: u64,
    },

    /// Stream was trimmed
    StreamTrimmed {
        /// Stream name
        stream: StreamName,
        /// Consensus group ID
        group_id: ConsensusGroupId,
        /// New start sequence
        new_start_seq: u64,
    },

    /// A consensus group was created
    GroupCreated {
        /// Group ID
        group_id: ConsensusGroupId,
        /// Initial members
        members: Vec<NodeId>,
    },

    /// A consensus group was deleted
    GroupDeleted {
        /// Group ID
        group_id: ConsensusGroupId,
    },

    /// Stream migration started
    StreamMigrationStarted {
        /// Stream name
        stream_name: StreamName,
        /// Source group
        from_group: ConsensusGroupId,
        /// Target group
        to_group: ConsensusGroupId,
    },

    /// Stream migration completed
    StreamMigrationCompleted {
        /// Stream name
        stream_name: StreamName,
        /// Source group
        from_group: ConsensusGroupId,
        /// Target group
        to_group: ConsensusGroupId,
    },

    /// Stream migration failed
    StreamMigrationFailed {
        /// Stream name
        stream_name: StreamName,
        /// Source group
        from_group: ConsensusGroupId,
        /// Target group
        to_group: ConsensusGroupId,
        /// Error message
        error: String,
    },

    /// Node added to group
    NodeAddedToGroup {
        /// Node ID
        node_id: NodeId,
        /// Group ID
        group_id: ConsensusGroupId,
    },

    /// Node removed from group
    NodeRemovedFromGroup {
        /// Node ID
        node_id: NodeId,
        /// Group ID
        group_id: ConsensusGroupId,
    },

    /// Node failed
    NodeFailed {
        /// Node ID
        node_id: NodeId,
        /// Error message
        error: String,
    },

    /// Node recovered
    NodeRecovered {
        /// Node ID
        node_id: NodeId,
    },

    /// Group health changed
    GroupHealthChanged {
        /// Group ID
        group_id: ConsensusGroupId,
        /// Old health status
        old_status: GroupHealthStatus,
        /// New health status
        new_status: GroupHealthStatus,
    },

    /// Membership changed in consensus
    MembershipChanged {
        /// New members
        new_members: Vec<NodeId>,
        /// Removed members
        removed_members: Vec<NodeId>,
    },

    /// Discovery service started
    DiscoveryStarted {
        /// Node ID that started discovery
        node_id: NodeId,
        /// Discovery mode (coordinator election, etc)
        mode: String,
    },

    /// Discovery completed successfully
    DiscoveryCompleted {
        /// Discovered nodes
        nodes: Vec<NodeId>,
        /// Elected coordinator (if applicable)
        coordinator: Option<NodeId>,
    },

    /// Discovery failed
    DiscoveryFailed {
        /// Error message
        error: String,
        /// Whether retry is possible
        can_retry: bool,
    },

    /// Coordinator elected
    CoordinatorElected {
        /// New coordinator node ID
        coordinator: NodeId,
        /// Term/epoch of election
        term: u64,
    },

    /// Global consensus initialized
    GlobalConsensusInitialized {
        /// Node ID
        node_id: NodeId,
        /// Initial members
        members: Vec<NodeId>,
    },

    /// Request to create default consensus group
    RequestDefaultGroupCreation {
        /// Members for the default group
        members: Vec<NodeId>,
    },

    /// Global consensus leader changed
    GlobalLeaderChanged {
        /// Previous leader (if any)
        old_leader: Option<NodeId>,
        /// New leader
        new_leader: NodeId,
        /// Term
        term: u64,
    },

    /// Group consensus initialized
    GroupConsensusInitialized {
        /// Group ID
        group_id: ConsensusGroupId,
        /// Node ID
        node_id: NodeId,
        /// Initial members
        members: Vec<NodeId>,
    },

    /// Group consensus leader changed
    GroupLeaderChanged {
        /// Group ID
        group_id: ConsensusGroupId,
        /// Previous leader (if any)
        old_leader: Option<NodeId>,
        /// New leader
        new_leader: NodeId,
        /// Term
        term: u64,
    },

    /// Global consensus state changed
    GlobalConsensusStateChanged {
        /// Old state
        old_state: String,
        /// New state
        new_state: String,
    },

    /// Group consensus state changed
    GroupConsensusStateChanged {
        /// Group ID
        group_id: ConsensusGroupId,
        /// Old state
        old_state: String,
        /// New state
        new_state: String,
    },

    /// Custom event for extensions
    Custom {
        /// Event type
        event_type: String,
        /// Event payload
        payload: serde_json::Value,
    },
}

/// Group health status
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum GroupHealthStatus {
    /// Group is healthy
    Healthy,
    /// Group is degraded
    Degraded,
    /// Group is unhealthy
    Unhealthy,
}

/// Event type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventType {
    /// Stream-related events
    Stream,
    /// Group-related events
    Group,
    /// Node-related events
    Node,
    /// Migration-related events
    Migration,
    /// Health-related events
    Health,
    /// Discovery-related events
    Discovery,
    /// Consensus-related events
    Consensus,
    /// Custom events
    Custom,
}

/// Event priority
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum EventPriority {
    /// Low priority
    Low,
    /// Normal priority
    Normal,
    /// High priority
    High,
    /// Critical priority
    Critical,
}

/// Event metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMetadata {
    /// Event ID
    pub id: EventId,
    /// Event timestamp
    pub timestamp: EventTimestamp,
    /// Event type
    pub event_type: EventType,
    /// Event priority
    pub priority: EventPriority,
    /// Source component
    pub source: String,
    /// Correlation ID for tracking related events
    pub correlation_id: Option<EventId>,
    /// Additional tags
    pub tags: Vec<String>,
}

/// Event envelope containing event and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// Event metadata
    pub metadata: EventMetadata,
    /// The actual event
    pub event: Event,
}

/// Result of an event handler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventResult {
    /// Operation succeeded
    Success,
    /// Operation failed with error
    Failed(String),
    /// Operation is pending/async
    Pending(EventId),
    /// Operation was ignored
    Ignored,
}

/// Reply channel for events
#[derive(Debug)]
pub struct ReplyChannel {
    inner: Arc<oneshot::Sender<EventResult>>,
}

impl ReplyChannel {
    /// Create a new reply channel
    pub fn new(sender: oneshot::Sender<EventResult>) -> Self {
        Self {
            inner: Arc::new(sender),
        }
    }

    /// Send a reply
    pub fn reply(self, result: EventResult) -> Result<(), EventResult> {
        Arc::try_unwrap(self.inner)
            .map_err(|_| result.clone())
            .and_then(|sender| sender.send(result))
    }
}

/// Event reply wrapper
pub struct EventReply {
    /// Reply channel
    pub channel: ReplyChannel,
    /// Timeout for reply
    pub timeout: Duration,
}

impl EventReply {
    /// Create a new event reply
    pub fn new(channel: ReplyChannel, timeout: Duration) -> Self {
        Self { channel, timeout }
    }

    /// Create a reply pair
    pub fn create_pair(timeout: Duration) -> (Self, oneshot::Receiver<EventResult>) {
        let (tx, rx) = oneshot::channel();
        let channel = ReplyChannel::new(tx);
        let reply = Self::new(channel, timeout);
        (reply, rx)
    }
}

/// Event configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventConfig {
    /// Event bus capacity
    pub bus_capacity: usize,

    /// Enable event persistence
    pub enable_persistence: bool,

    /// Event retention duration
    pub retention_duration: Duration,

    /// Max subscribers per event type
    pub max_subscribers: usize,

    /// Event processing timeout
    pub processing_timeout: Duration,

    /// Enable event deduplication
    pub enable_deduplication: bool,

    /// Deduplication window
    pub deduplication_window: Duration,

    /// Enable metrics collection
    pub enable_metrics: bool,
}

impl Default for EventConfig {
    fn default() -> Self {
        Self {
            bus_capacity: 10000,
            enable_persistence: false,
            retention_duration: Duration::from_secs(86400), // 24 hours
            max_subscribers: 100,
            processing_timeout: Duration::from_secs(30),
            enable_deduplication: true,
            deduplication_window: Duration::from_secs(60),
            enable_metrics: true,
        }
    }
}

impl Event {
    /// Get the event type
    pub fn event_type(&self) -> EventType {
        match self {
            Event::StreamCreated { .. }
            | Event::StreamDeleted { .. }
            | Event::StreamConfigUpdated { .. }
            | Event::StreamReallocated { .. }
            | Event::StreamMessageAppended { .. }
            | Event::StreamTrimmed { .. } => EventType::Stream,

            Event::GroupCreated { .. } | Event::GroupDeleted { .. } => EventType::Group,

            Event::NodeAddedToGroup { .. }
            | Event::NodeRemovedFromGroup { .. }
            | Event::NodeFailed { .. }
            | Event::NodeRecovered { .. } => EventType::Node,

            Event::StreamMigrating { .. }
            | Event::StreamMigrationStarted { .. }
            | Event::StreamMigrationCompleted { .. }
            | Event::StreamMigrationFailed { .. } => EventType::Migration,

            Event::GroupHealthChanged { .. } | Event::MembershipChanged { .. } => EventType::Health,

            Event::DiscoveryStarted { .. }
            | Event::DiscoveryCompleted { .. }
            | Event::DiscoveryFailed { .. }
            | Event::CoordinatorElected { .. } => EventType::Discovery,

            Event::GlobalConsensusInitialized { .. }
            | Event::RequestDefaultGroupCreation { .. }
            | Event::GlobalLeaderChanged { .. }
            | Event::GroupConsensusInitialized { .. }
            | Event::GroupLeaderChanged { .. }
            | Event::GlobalConsensusStateChanged { .. }
            | Event::GroupConsensusStateChanged { .. } => EventType::Consensus,

            Event::Custom { .. } => EventType::Custom,
        }
    }

    /// Get the default priority for this event
    pub fn default_priority(&self) -> EventPriority {
        match self {
            Event::NodeFailed { .. }
            | Event::StreamMigrationFailed { .. }
            | Event::DiscoveryFailed { .. } => EventPriority::Critical,

            Event::StreamMigrating { .. }
            | Event::GroupHealthChanged { .. }
            | Event::MembershipChanged { .. }
            | Event::CoordinatorElected { .. }
            | Event::StreamMessageAppended { .. }
            | Event::GlobalLeaderChanged { .. }
            | Event::GroupLeaderChanged { .. }
            | Event::GlobalConsensusStateChanged { .. }
            | Event::GroupConsensusStateChanged { .. } => EventPriority::High,

            Event::StreamCreated { .. }
            | Event::StreamDeleted { .. }
            | Event::GroupCreated { .. }
            | Event::GroupDeleted { .. }
            | Event::DiscoveryStarted { .. }
            | Event::DiscoveryCompleted { .. }
            | Event::GlobalConsensusInitialized { .. }
            | Event::RequestDefaultGroupCreation { .. }
            | Event::GroupConsensusInitialized { .. } => EventPriority::Normal,

            _ => EventPriority::Low,
        }
    }
}
