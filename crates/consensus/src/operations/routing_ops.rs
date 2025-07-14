//! Routing and subscription operations
//!
//! This module contains operations for managing message routing
//! and stream subscriptions in the PubSub system.

use crate::error::{ConsensusResult, Error};
use serde::{Deserialize, Serialize};

/// Operations related to routing and subscriptions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RoutingOperation {
    /// Subscribe a stream to a subject pattern
    Subscribe {
        /// Stream name
        stream_name: String,
        /// Subject pattern to subscribe to
        subject_pattern: String,
    },

    /// Unsubscribe a stream from a subject pattern
    Unsubscribe {
        /// Stream name
        stream_name: String,
        /// Subject pattern to unsubscribe from
        subject_pattern: String,
    },

    /// Remove all subscriptions for a stream
    RemoveAllSubscriptions {
        /// Stream name
        stream_name: String,
    },

    /// Bulk subscribe to multiple patterns
    BulkSubscribe {
        /// Stream name
        stream_name: String,
        /// Subject patterns to subscribe to
        subject_patterns: Vec<String>,
    },

    /// Bulk unsubscribe from multiple patterns
    BulkUnsubscribe {
        /// Stream name
        stream_name: String,
        /// Subject patterns to unsubscribe from
        subject_patterns: Vec<String>,
    },
}

impl RoutingOperation {
    /// Get the stream name this operation affects
    pub fn stream_name(&self) -> &str {
        match self {
            Self::Subscribe { stream_name, .. }
            | Self::Unsubscribe { stream_name, .. }
            | Self::RemoveAllSubscriptions { stream_name }
            | Self::BulkSubscribe { stream_name, .. }
            | Self::BulkUnsubscribe { stream_name, .. } => stream_name,
        }
    }

    /// Get a human-readable operation name
    pub fn operation_name(&self) -> String {
        match self {
            Self::Subscribe { .. } => "subscribe".to_string(),
            Self::Unsubscribe { .. } => "unsubscribe".to_string(),
            Self::RemoveAllSubscriptions { .. } => "remove_all_subscriptions".to_string(),
            Self::BulkSubscribe { .. } => "bulk_subscribe".to_string(),
            Self::BulkUnsubscribe { .. } => "bulk_unsubscribe".to_string(),
        }
    }

    /// Check if this operation adds subscriptions
    pub fn adds_subscriptions(&self) -> bool {
        matches!(self, Self::Subscribe { .. } | Self::BulkSubscribe { .. })
    }

    /// Check if this operation removes subscriptions
    pub fn removes_subscriptions(&self) -> bool {
        matches!(
            self,
            Self::Unsubscribe { .. }
                | Self::RemoveAllSubscriptions { .. }
                | Self::BulkUnsubscribe { .. }
        )
    }

    /// Get all subject patterns affected by this operation
    pub fn subject_patterns(&self) -> Vec<&str> {
        match self {
            Self::Subscribe {
                subject_pattern, ..
            }
            | Self::Unsubscribe {
                subject_pattern, ..
            } => vec![subject_pattern.as_str()],
            Self::RemoveAllSubscriptions { .. } => vec![],
            Self::BulkSubscribe {
                subject_patterns, ..
            }
            | Self::BulkUnsubscribe {
                subject_patterns, ..
            } => subject_patterns.iter().map(|s| s.as_str()).collect(),
        }
    }

    /// Validate subject pattern format
    pub fn validate_subject_pattern(pattern: &str) -> ConsensusResult<()> {
        if pattern.is_empty() {
            return Err(Error::InvalidOperation(
                "Subject pattern cannot be empty".to_string(),
            ));
        }

        if pattern.len() > 255 {
            return Err(Error::InvalidOperation(
                "Subject pattern cannot exceed 255 characters".to_string(),
            ));
        }

        // Validate pattern segments
        let segments: Vec<&str> = pattern.split('.').collect();
        for (idx, segment) in segments.iter().enumerate() {
            if segment.is_empty() {
                return Err(Error::InvalidOperation(
                    "Subject pattern cannot have empty segments".to_string(),
                ));
            }

            // Check for valid characters in segment
            for ch in segment.chars() {
                match ch {
                    'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '*' | '>' => {}
                    _ => {
                        return Err(Error::InvalidOperation(format!(
                            "Invalid character '{ch}' in subject pattern"
                        )));
                    }
                }
            }

            // Validate wildcards
            if segment.contains('*') && *segment != "*" {
                return Err(Error::InvalidOperation(
                    "Wildcard '*' must be the only character in a segment".to_string(),
                ));
            }

            if segment.contains('>') {
                if *segment != ">" {
                    return Err(Error::InvalidOperation(
                        "Multi-level wildcard '>' must be the only character in a segment"
                            .to_string(),
                    ));
                }
                // '>' must be the last segment
                if idx != segments.len() - 1 {
                    return Err(Error::InvalidOperation(
                        "Multi-level wildcard '>' can only appear at the end".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Check if a subject matches a pattern
    pub fn matches_pattern(subject: &str, pattern: &str) -> bool {
        let subject_parts: Vec<&str> = subject.split('.').collect();
        let pattern_parts: Vec<&str> = pattern.split('.').collect();

        let mut s_idx = 0;
        let mut p_idx = 0;

        while s_idx < subject_parts.len() && p_idx < pattern_parts.len() {
            let pattern_part = pattern_parts[p_idx];

            match pattern_part {
                ">" => return true, // Multi-level wildcard matches everything remaining
                "*" => {
                    // Single-level wildcard matches any single token
                    s_idx += 1;
                    p_idx += 1;
                }
                _ => {
                    // Literal match required
                    if subject_parts[s_idx] != pattern_part {
                        return false;
                    }
                    s_idx += 1;
                    p_idx += 1;
                }
            }
        }

        // Both must be exhausted for a match (unless pattern ended with >)
        s_idx == subject_parts.len() && p_idx == pattern_parts.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subject_pattern_validation() {
        // Valid patterns
        assert!(RoutingOperation::validate_subject_pattern("orders.new").is_ok());
        assert!(RoutingOperation::validate_subject_pattern("orders.*").is_ok());
        assert!(RoutingOperation::validate_subject_pattern("orders.>").is_ok());
        assert!(RoutingOperation::validate_subject_pattern("orders.*.shipped").is_ok());

        // Invalid patterns
        assert!(RoutingOperation::validate_subject_pattern("").is_err());
        assert!(RoutingOperation::validate_subject_pattern("orders..new").is_err());
        assert!(RoutingOperation::validate_subject_pattern("orders.n*w").is_err());
        assert!(RoutingOperation::validate_subject_pattern("orders.>.new").is_err());
        assert!(RoutingOperation::validate_subject_pattern("orders.new!").is_err());
    }

    #[test]
    fn test_pattern_matching() {
        // Exact match
        assert!(RoutingOperation::matches_pattern(
            "orders.new",
            "orders.new"
        ));
        assert!(!RoutingOperation::matches_pattern(
            "orders.old",
            "orders.new"
        ));

        // Single wildcard
        assert!(RoutingOperation::matches_pattern("orders.new", "orders.*"));
        assert!(RoutingOperation::matches_pattern("orders.old", "orders.*"));
        assert!(!RoutingOperation::matches_pattern(
            "orders.new.urgent",
            "orders.*"
        ));

        // Multi-level wildcard
        assert!(RoutingOperation::matches_pattern("orders.new", "orders.>"));
        assert!(RoutingOperation::matches_pattern(
            "orders.new.urgent",
            "orders.>"
        ));
        assert!(RoutingOperation::matches_pattern(
            "orders.new.urgent.customer",
            "orders.>"
        ));

        // Complex patterns
        assert!(RoutingOperation::matches_pattern(
            "orders.new.shipped",
            "orders.*.shipped"
        ));
        assert!(!RoutingOperation::matches_pattern(
            "orders.new.pending",
            "orders.*.shipped"
        ));
    }

    #[test]
    fn test_operation_properties() {
        let subscribe_op = RoutingOperation::Subscribe {
            stream_name: "test-stream".to_string(),
            subject_pattern: "orders.*".to_string(),
        };

        assert_eq!(subscribe_op.stream_name(), "test-stream");
        assert_eq!(subscribe_op.operation_name(), "subscribe");
        assert!(subscribe_op.adds_subscriptions());
        assert!(!subscribe_op.removes_subscriptions());
        assert_eq!(subscribe_op.subject_patterns(), vec!["orders.*"]);
    }
}
