//! Stream management operations
//!
//! This module contains all operations related to stream lifecycle management,
//! including creation, configuration updates, deletion, and migration.
//! These are administrative operations on streams, not data operations within streams.

use crate::{
    allocation::ConsensusGroupId,
    error::{ConsensusResult, Error, StreamError},
    global::StreamConfig,
    local::MigrationState,
};
use serde::{Deserialize, Serialize};

/// Operations related to stream lifecycle management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamManagementOperation {
    /// Create a new stream
    Create {
        /// Stream name
        name: String,
        /// Stream configuration
        config: StreamConfig,
        /// Target consensus group
        group_id: ConsensusGroupId,
    },

    /// Update stream configuration
    UpdateConfig {
        /// Stream name
        name: String,
        /// New configuration
        config: StreamConfig,
    },

    /// Delete a stream
    Delete {
        /// Stream name
        name: String,
    },

    /// Reallocate stream to a different group
    Reallocate {
        /// Stream name
        name: String,
        /// Target consensus group
        target_group: ConsensusGroupId,
    },

    /// Migrate stream between groups
    Migrate {
        /// Stream name
        name: String,
        /// Source group
        from_group: ConsensusGroupId,
        /// Target group
        to_group: ConsensusGroupId,
        /// Migration state
        state: MigrationState,
    },

    /// Update stream allocation after migration
    UpdateAllocation {
        /// Stream name
        name: String,
        /// New consensus group
        new_group: ConsensusGroupId,
    },
}

impl StreamManagementOperation {
    /// Get the stream name this operation affects
    pub fn stream_name(&self) -> &str {
        match self {
            Self::Create { name, .. }
            | Self::UpdateConfig { name, .. }
            | Self::Delete { name }
            | Self::Reallocate { name, .. }
            | Self::Migrate { name, .. }
            | Self::UpdateAllocation { name, .. } => name,
        }
    }

    /// Get a human-readable operation name
    pub fn operation_name(&self) -> String {
        match self {
            Self::Create { .. } => "create_stream".to_string(),
            Self::UpdateConfig { .. } => "update_stream_config".to_string(),
            Self::Delete { .. } => "delete_stream".to_string(),
            Self::Reallocate { .. } => "reallocate_stream".to_string(),
            Self::Migrate { .. } => "migrate_stream".to_string(),
            Self::UpdateAllocation { .. } => "update_stream_allocation".to_string(),
        }
    }

    /// Check if this operation requires the stream to exist
    pub fn requires_existing_stream(&self) -> bool {
        !matches!(self, Self::Create { .. })
    }

    /// Check if this operation modifies stream allocation
    pub fn modifies_allocation(&self) -> bool {
        matches!(
            self,
            Self::Create { .. }
                | Self::Reallocate { .. }
                | Self::Migrate { .. }
                | Self::UpdateAllocation { .. }
        )
    }

    /// Validate stream name format
    pub fn validate_stream_name(name: &str) -> ConsensusResult<()> {
        if name.is_empty() {
            return Err(Error::Stream(StreamError::InvalidName {
                name: name.to_string(),
                reason: "Stream name cannot be empty".to_string(),
            }));
        }

        if name.len() > 255 {
            return Err(Error::Stream(StreamError::InvalidName {
                name: name.to_string(),
                reason: "Stream name cannot exceed 255 characters".to_string(),
            }));
        }

        // Check for valid characters
        if !name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_' || c == '.')
        {
            return Err(Error::Stream(StreamError::InvalidName {
                name: name.to_string(),
                reason: "Stream name can only contain alphanumeric characters, hyphens, underscores, and dots".to_string(),
            }));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_name_validation() {
        // Valid names
        assert!(StreamManagementOperation::validate_stream_name("test-stream").is_ok());
        assert!(StreamManagementOperation::validate_stream_name("test_stream").is_ok());
        assert!(StreamManagementOperation::validate_stream_name("test.stream").is_ok());
        assert!(StreamManagementOperation::validate_stream_name("test123").is_ok());

        // Invalid names
        assert!(StreamManagementOperation::validate_stream_name("").is_err());
        assert!(StreamManagementOperation::validate_stream_name("test stream").is_err());
        assert!(StreamManagementOperation::validate_stream_name("test@stream").is_err());
        assert!(StreamManagementOperation::validate_stream_name(&"a".repeat(256)).is_err());
    }

    #[test]
    fn test_operation_properties() {
        let create_op = StreamManagementOperation::Create {
            name: "test".to_string(),
            config: StreamConfig::default(),
            group_id: ConsensusGroupId::new(1),
        };

        assert_eq!(create_op.stream_name(), "test");
        assert_eq!(create_op.operation_name(), "create_stream");
        assert!(!create_op.requires_existing_stream());
        assert!(create_op.modifies_allocation());

        let update_op = StreamManagementOperation::UpdateConfig {
            name: "test".to_string(),
            config: StreamConfig::default(),
        };

        assert!(update_op.requires_existing_stream());
        assert!(!update_op.modifies_allocation());
    }
}
