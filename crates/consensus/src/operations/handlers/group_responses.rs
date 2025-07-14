//! Type-safe response types for group stream operations
//!
//! This module defines response enums that correspond to each operation type
//! in the group consensus layer, providing type safety and better error handling.

use crate::ConsensusGroupId;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Response from group stream operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GroupStreamOperationResponse {
    /// Stream data operation response
    Stream(StreamOperationResponse),
    /// Migration operation response
    Migration(MigrationOperationResponse),
    /// PubSub operation response
    PubSub(PubSubOperationResponse),
    /// Maintenance operation response
    Maintenance(MaintenanceOperationResponse),
}

impl GroupStreamOperationResponse {
    /// Whether the operation succeeded
    pub fn is_success(&self) -> bool {
        match self {
            Self::Stream(r) => r.is_success(),
            Self::Migration(r) => r.is_success(),
            Self::PubSub(r) => r.is_success(),
            Self::Maintenance(r) => r.is_success(),
        }
    }

    /// Get the sequence number if applicable
    pub fn sequence(&self) -> Option<u64> {
        match self {
            Self::Stream(r) => r.sequence(),
            Self::Migration(r) => r.sequence(),
            Self::PubSub(r) => r.sequence(),
            Self::Maintenance(r) => r.sequence(),
        }
    }

    /// Get any error message
    pub fn error(&self) -> Option<&str> {
        match self {
            Self::Stream(r) => r.error(),
            Self::Migration(r) => r.error(),
            Self::PubSub(r) => r.error(),
            Self::Maintenance(r) => r.error(),
        }
    }

    /// Get data if this is a read operation
    pub fn data(&self) -> Option<&Bytes> {
        match self {
            Self::Stream(StreamOperationResponse::Read { data, .. }) => Some(data),
            Self::PubSub(PubSubOperationResponse::MessageRetrieved { data, .. }) => Some(data),
            _ => None,
        }
    }
}

/// Response from stream data operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamOperationResponse {
    /// Data was written to stream
    Written {
        /// The sequence number assigned
        sequence: u64,
        /// The stream name
        stream_name: String,
        /// Size of data written
        data_size: usize,
    },
    /// Data was read from stream
    Read {
        /// The sequence number
        sequence: u64,
        /// The stream name
        stream_name: String,
        /// The data
        data: Bytes,
        /// Associated metadata
        metadata: Option<HashMap<String, String>>,
    },
    /// Checkpoint was created
    CheckpointCreated {
        /// The sequence number
        sequence: u64,
        /// The stream name
        stream_name: String,
        /// Checkpoint data
        checkpoint_data: Bytes,
    },
    /// Stream was truncated
    Truncated {
        /// The sequence number
        sequence: u64,
        /// The stream name
        stream_name: String,
        /// Number of messages removed
        messages_removed: usize,
    },
    /// Operation failed
    Failed {
        /// The operation that failed
        operation: String,
        /// The reason for failure
        reason: String,
    },
}

impl StreamOperationResponse {
    pub fn is_success(&self) -> bool {
        !matches!(self, Self::Failed { .. })
    }

    pub fn sequence(&self) -> Option<u64> {
        match self {
            Self::Written { sequence, .. } => Some(*sequence),
            Self::Read { sequence, .. } => Some(*sequence),
            Self::CheckpointCreated { sequence, .. } => Some(*sequence),
            Self::Truncated { sequence, .. } => Some(*sequence),
            Self::Failed { .. } => None,
        }
    }

    pub fn error(&self) -> Option<&str> {
        match self {
            Self::Failed { reason, .. } => Some(reason),
            _ => None,
        }
    }
}

/// Response from migration operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MigrationOperationResponse {
    /// Migration was initiated
    Initiated {
        /// The sequence number
        sequence: u64,
        /// Stream being migrated
        stream_name: String,
        /// Source group
        source_group: ConsensusGroupId,
        /// Target group
        target_group: ConsensusGroupId,
    },
    /// Snapshot was transferred
    SnapshotTransferred {
        /// The sequence number
        sequence: u64,
        /// Stream name
        stream_name: String,
        /// Messages transferred
        messages_count: usize,
        /// Bytes transferred
        bytes_transferred: usize,
    },
    /// Migration was completed
    Completed {
        /// The sequence number
        sequence: u64,
        /// Stream name
        stream_name: String,
        /// Total messages migrated
        total_messages: usize,
        /// Duration in milliseconds
        duration_ms: u64,
    },
    /// Migration was cancelled
    Cancelled {
        /// The sequence number
        sequence: u64,
        /// Stream name
        stream_name: String,
        /// Reason for cancellation
        reason: String,
    },
    /// Operation failed
    Failed {
        /// The operation that failed
        operation: String,
        /// The reason for failure
        reason: String,
    },
}

impl MigrationOperationResponse {
    pub fn is_success(&self) -> bool {
        !matches!(self, Self::Failed { .. })
    }

    pub fn sequence(&self) -> Option<u64> {
        match self {
            Self::Initiated { sequence, .. } => Some(*sequence),
            Self::SnapshotTransferred { sequence, .. } => Some(*sequence),
            Self::Completed { sequence, .. } => Some(*sequence),
            Self::Cancelled { sequence, .. } => Some(*sequence),
            Self::Failed { .. } => None,
        }
    }

    pub fn error(&self) -> Option<&str> {
        match self {
            Self::Failed { reason, .. } => Some(reason),
            Self::Cancelled { reason, .. } => Some(reason),
            _ => None,
        }
    }
}

/// Response from PubSub operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PubSubOperationResponse {
    /// Message was published
    Published {
        /// The sequence number
        sequence: u64,
        /// The stream name
        stream_name: String,
        /// The subject
        subject: String,
        /// Number of subscribers notified
        subscribers_notified: usize,
    },
    /// Message was retrieved
    MessageRetrieved {
        /// The sequence number
        sequence: u64,
        /// The stream name
        stream_name: String,
        /// The data
        data: Bytes,
        /// The subject
        subject: String,
    },
    /// Acknowledgment was recorded
    Acknowledged {
        /// The sequence number
        sequence: u64,
        /// The stream name
        stream_name: String,
        /// The consumer
        consumer: String,
    },
    /// Operation failed
    Failed {
        /// The operation that failed
        operation: String,
        /// The reason for failure
        reason: String,
    },
}

impl PubSubOperationResponse {
    pub fn is_success(&self) -> bool {
        !matches!(self, Self::Failed { .. })
    }

    pub fn sequence(&self) -> Option<u64> {
        match self {
            Self::Published { sequence, .. } => Some(*sequence),
            Self::MessageRetrieved { sequence, .. } => Some(*sequence),
            Self::Acknowledged { sequence, .. } => Some(*sequence),
            Self::Failed { .. } => None,
        }
    }

    pub fn error(&self) -> Option<&str> {
        match self {
            Self::Failed { reason, .. } => Some(reason),
            _ => None,
        }
    }
}

/// Response from maintenance operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MaintenanceOperationResponse {
    /// Compaction was performed
    Compacted {
        /// The sequence number
        sequence: u64,
        /// Number of entries before
        entries_before: usize,
        /// Number of entries after
        entries_after: usize,
        /// Bytes reclaimed
        bytes_reclaimed: usize,
    },
    /// Verification was performed
    Verified {
        /// The sequence number
        sequence: u64,
        /// Number of entries checked
        entries_checked: usize,
        /// Number of errors found
        errors_found: usize,
        /// List of errors
        errors: Vec<String>,
    },
    /// Repair was performed
    Repaired {
        /// The sequence number
        sequence: u64,
        /// Number of issues fixed
        issues_fixed: usize,
        /// Description of repairs
        repairs: Vec<String>,
    },
    /// Membership was updated
    MembershipUpdated {
        /// The sequence number
        sequence: u64,
        /// The group ID
        group_id: ConsensusGroupId,
        /// The membership configuration
        membership_config: String,
    },
    /// Heartbeat received
    Heartbeat {
        /// The sequence number
        sequence: u64,
        /// The group ID
        group_id: ConsensusGroupId,
    },
    /// Operation failed
    Failed {
        /// The operation that failed
        operation: String,
        /// The reason for failure
        reason: String,
    },
}

impl MaintenanceOperationResponse {
    pub fn is_success(&self) -> bool {
        !matches!(self, Self::Failed { .. })
    }

    pub fn sequence(&self) -> Option<u64> {
        match self {
            Self::Compacted { sequence, .. } => Some(*sequence),
            Self::Verified { sequence, .. } => Some(*sequence),
            Self::Repaired { sequence, .. } => Some(*sequence),
            Self::MembershipUpdated { sequence, .. } => Some(*sequence),
            Self::Heartbeat { sequence, .. } => Some(*sequence),
            Self::Failed { .. } => None,
        }
    }

    pub fn error(&self) -> Option<&str> {
        match self {
            Self::Failed { reason, .. } => Some(reason),
            _ => None,
        }
    }
}

// Implement conversions from specific responses to GroupStreamOperationResponse
impl From<StreamOperationResponse> for GroupStreamOperationResponse {
    fn from(resp: StreamOperationResponse) -> Self {
        GroupStreamOperationResponse::Stream(resp)
    }
}

impl From<MigrationOperationResponse> for GroupStreamOperationResponse {
    fn from(resp: MigrationOperationResponse) -> Self {
        GroupStreamOperationResponse::Migration(resp)
    }
}

impl From<PubSubOperationResponse> for GroupStreamOperationResponse {
    fn from(resp: PubSubOperationResponse) -> Self {
        GroupStreamOperationResponse::PubSub(resp)
    }
}

impl From<MaintenanceOperationResponse> for GroupStreamOperationResponse {
    fn from(resp: MaintenanceOperationResponse) -> Self {
        GroupStreamOperationResponse::Maintenance(resp)
    }
}

/// Convert from legacy GroupResponse to GroupStreamOperationResponse
/// This is a compatibility layer that will be removed once all code is migrated
impl From<crate::core::group::GroupResponse> for GroupStreamOperationResponse {
    fn from(old: crate::core::group::GroupResponse) -> Self {
        if old.success {
            // Try to determine the operation type from the response data
            if let Some(data) = old.data {
                // This is likely a read operation
                GroupStreamOperationResponse::Stream(StreamOperationResponse::Read {
                    sequence: old.sequence.unwrap_or(0),
                    stream_name: String::new(), // We don't have this info
                    data,
                    metadata: None,
                })
            } else if let Some(checkpoint_data) = old.checkpoint_data {
                // This is a checkpoint operation
                GroupStreamOperationResponse::Stream(StreamOperationResponse::CheckpointCreated {
                    sequence: old.sequence.unwrap_or(0),
                    stream_name: String::new(),
                    checkpoint_data,
                })
            } else if let Some(seq) = old.sequence {
                // This is likely a write operation
                GroupStreamOperationResponse::Stream(StreamOperationResponse::Written {
                    sequence: seq,
                    stream_name: String::new(),
                    data_size: 0, // We don't have this info
                })
            } else {
                // Generic success - treat as maintenance
                GroupStreamOperationResponse::Maintenance(MaintenanceOperationResponse::Verified {
                    sequence: 0,
                    entries_checked: 0,
                    errors_found: 0,
                    errors: vec![],
                })
            }
        } else {
            GroupStreamOperationResponse::Stream(StreamOperationResponse::Failed {
                operation: "LegacyOperation".to_string(),
                reason: old.error.unwrap_or_else(|| "Unknown error".to_string()),
            })
        }
    }
}

/// Convert GroupStreamOperationResponse back to GroupResponse for compatibility
impl From<GroupStreamOperationResponse> for crate::core::group::GroupResponse {
    fn from(resp: GroupStreamOperationResponse) -> Self {
        crate::core::group::GroupResponse {
            success: resp.is_success(),
            sequence: resp.sequence(),
            error: resp.error().map(|s| s.to_string()),
            checkpoint_data: match &resp {
                GroupStreamOperationResponse::Stream(
                    StreamOperationResponse::CheckpointCreated {
                        checkpoint_data, ..
                    },
                ) => Some(checkpoint_data.clone()),
                _ => None,
            },
            data: resp.data().cloned(),
        }
    }
}
