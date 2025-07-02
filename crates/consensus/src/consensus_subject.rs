//! Subject-based messaging for consensus streams.
//!
//! This module provides support for publishing messages to subjects that can be
//! routed to multiple streams based on pattern matching.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// A subject represents a routing key for messages in the consensus system.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConsensusSubject {
    /// The subject string (e.g., "orders.new", "users.*.profile")
    pub name: String,
}

impl ConsensusSubject {
    /// Create a new subject with the given name.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// Get the subject name.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Check if this subject matches a given pattern.
    /// Supports wildcard patterns:
    /// - `*` matches any single token
    /// - `>` matches one or more tokens at the end
    #[must_use]
    pub fn matches_pattern(&self, pattern: &str) -> bool {
        subject_matches_pattern(&self.name, pattern)
    }
}

/// A router that maintains mappings between subjects and streams.
#[derive(Debug, Clone, Default)]
pub struct SubjectRouter {
    /// Maps from subject patterns to sets of stream names
    subject_to_streams: HashMap<String, HashSet<String>>,
    /// Maps from stream names to sets of subject patterns they subscribe to
    stream_to_subjects: HashMap<String, HashSet<String>>,
}

impl SubjectRouter {
    /// Create a new empty subject router.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a stream to receive messages from a subject pattern.
    pub fn subscribe_stream(
        &mut self,
        stream_name: impl Into<String>,
        subject_pattern: impl Into<String>,
    ) {
        let stream_name = stream_name.into();
        let subject_pattern = subject_pattern.into();

        // Add stream to the subject's routing list
        self.subject_to_streams
            .entry(subject_pattern.clone())
            .or_default()
            .insert(stream_name.clone());

        // Add subject to the stream's subscription list
        self.stream_to_subjects
            .entry(stream_name)
            .or_default()
            .insert(subject_pattern);
    }

    /// Unregister a stream from a subject pattern.
    pub fn unsubscribe_stream(&mut self, stream_name: &str, subject_pattern: &str) {
        if let Some(streams) = self.subject_to_streams.get_mut(subject_pattern) {
            streams.remove(stream_name);
            if streams.is_empty() {
                self.subject_to_streams.remove(subject_pattern);
            }
        }

        if let Some(subjects) = self.stream_to_subjects.get_mut(stream_name) {
            subjects.remove(subject_pattern);
            if subjects.is_empty() {
                self.stream_to_subjects.remove(stream_name);
            }
        }
    }

    /// Find all streams that should receive a message published to a given subject.
    #[must_use]
    pub fn route_subject(&self, subject: &str) -> HashSet<String> {
        let mut matching_streams = HashSet::new();

        // Check each subject pattern to see if it matches the given subject
        for (pattern, streams) in &self.subject_to_streams {
            if subject_matches_pattern(subject, pattern) {
                matching_streams.extend(streams.clone());
            }
        }

        matching_streams
    }

    /// Get all subject patterns that a stream is subscribed to.
    #[must_use]
    pub fn get_stream_subjects(&self, stream_name: &str) -> Option<&HashSet<String>> {
        self.stream_to_subjects.get(stream_name)
    }

    /// Get all streams subscribed to a specific subject pattern.
    #[must_use]
    pub fn get_subject_streams(&self, subject_pattern: &str) -> Option<&HashSet<String>> {
        self.subject_to_streams.get(subject_pattern)
    }

    /// Remove all subscriptions for a stream.
    pub fn remove_stream(&mut self, stream_name: &str) {
        if let Some(subjects) = self.stream_to_subjects.remove(stream_name) {
            for subject in subjects {
                if let Some(streams) = self.subject_to_streams.get_mut(&subject) {
                    streams.remove(stream_name);
                    if streams.is_empty() {
                        self.subject_to_streams.remove(&subject);
                    }
                }
            }
        }
    }

    /// Get a summary of all current subscriptions.
    #[must_use]
    pub const fn get_subscriptions(&self) -> &HashMap<String, HashSet<String>> {
        &self.subject_to_streams
    }
}

/// Check if a subject matches a pattern.
/// Supports NATS-style wildcards:
/// - `*` matches exactly one token
/// - `>` matches one or more tokens (and must be at the end)
#[must_use]
pub fn subject_matches_pattern(subject: &str, pattern: &str) -> bool {
    // Handle exact match case
    if subject == pattern {
        return true;
    }

    let subject_tokens: Vec<&str> = subject.split('.').collect();
    let pattern_tokens: Vec<&str> = pattern.split('.').collect();

    // Handle `>` wildcard (must be at the end)
    if let Some(last_pattern_token) = pattern_tokens.last() {
        if *last_pattern_token == ">" {
            // Pattern ends with `>`, so it can match one or more remaining tokens
            // We need at least pattern_tokens.len() tokens in the subject
            if pattern_tokens.len() <= subject_tokens.len() {
                // Check if all tokens before `>` match
                for (i, pattern_token) in pattern_tokens
                    .iter()
                    .take(pattern_tokens.len() - 1)
                    .enumerate()
                {
                    if let Some(subject_token) = subject_tokens.get(i) {
                        if *pattern_token != "*" && *pattern_token != *subject_token {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }
    }

    // Handle exact token count matching with `*` wildcards
    if subject_tokens.len() != pattern_tokens.len() {
        return false;
    }

    // Check each token
    for (subject_token, pattern_token) in subject_tokens.iter().zip(pattern_tokens.iter()) {
        if *pattern_token != "*" && *pattern_token != *subject_token {
            return false;
        }
    }

    true
}

/// Validation error for subject patterns
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubjectValidationError {
    /// Subject pattern is empty
    Empty,
    /// Subject pattern contains invalid characters
    InvalidCharacters(String),
    /// Subject pattern has invalid wildcard usage
    InvalidWildcard(String),
    /// Subject pattern is too long
    TooLong(usize),
}

impl std::fmt::Display for SubjectValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "Subject pattern cannot be empty"),
            Self::InvalidCharacters(chars) => {
                write!(f, "Invalid characters in subject pattern: {chars}")
            }
            Self::InvalidWildcard(msg) => write!(f, "Invalid wildcard usage: {msg}"),
            Self::TooLong(len) => {
                write!(f, "Subject pattern too long: {len} characters (max 1024)")
            }
        }
    }
}

impl std::error::Error for SubjectValidationError {}

/// Validate a subject pattern for correctness
///
/// # Errors
///
/// Returns `SubjectValidationError` if the pattern is invalid.
pub fn validate_subject_pattern(pattern: &str) -> Result<(), SubjectValidationError> {
    // Check for empty pattern
    if pattern.is_empty() {
        return Err(SubjectValidationError::Empty);
    }

    // Check length limit (NATS typically limits to 1024 characters)
    if pattern.len() > 1024 {
        return Err(SubjectValidationError::TooLong(pattern.len()));
    }

    // Check for invalid characters (only alphanumeric, dots, wildcards, hyphens, underscores)
    let invalid_chars: Vec<char> = pattern
        .chars()
        .filter(|&c| {
            !c.is_alphanumeric() && c != '.' && c != '*' && c != '>' && c != '-' && c != '_'
        })
        .collect();

    if !invalid_chars.is_empty() {
        return Err(SubjectValidationError::InvalidCharacters(
            invalid_chars.iter().collect::<String>(),
        ));
    }

    // Validate wildcard usage
    let tokens: Vec<&str> = pattern.split('.').collect();

    for (i, token) in tokens.iter().enumerate() {
        // Check for empty tokens (consecutive dots)
        if token.is_empty() {
            return Err(SubjectValidationError::InvalidWildcard(
                "Empty tokens (consecutive dots) not allowed".to_string(),
            ));
        }

        // Check for mixed wildcard/literal in same token
        if token.len() > 1 && (token.contains('*') || token.contains('>')) {
            return Err(SubjectValidationError::InvalidWildcard(format!(
                "Mixed wildcard and literal characters in token: '{token}'"
            )));
        }

        // Check that '>' only appears at the end
        if *token == ">" && i != tokens.len() - 1 {
            return Err(SubjectValidationError::InvalidWildcard(
                "'>' wildcard can only appear at the end of pattern".to_string(),
            ));
        }

        // Check for invalid '>' usage within tokens
        if token.contains('>') && *token != ">" {
            return Err(SubjectValidationError::InvalidWildcard(format!(
                "Invalid '>' usage in token: '{token}'"
            )));
        }
    }

    Ok(())
}

/// Validate a stream name for correctness
///
/// # Errors
///
/// Returns `SubjectValidationError` if the stream name is invalid.
pub fn validate_stream_name(name: &str) -> Result<(), SubjectValidationError> {
    // Check for empty name
    if name.is_empty() {
        return Err(SubjectValidationError::Empty);
    }

    // Check length limit
    if name.len() > 256 {
        return Err(SubjectValidationError::TooLong(name.len()));
    }

    // Stream names are more restrictive - no wildcards allowed
    let invalid_chars: Vec<char> = name
        .chars()
        .filter(|&c| !c.is_alphanumeric() && c != '-' && c != '_')
        .collect();

    if !invalid_chars.is_empty() {
        return Err(SubjectValidationError::InvalidCharacters(
            invalid_chars.iter().collect::<String>(),
        ));
    }

    // Stream names cannot start with underscore (reserved for system streams)
    if name.starts_with('_') {
        return Err(SubjectValidationError::InvalidCharacters(
            "Stream names cannot start with '_' (reserved for system streams)".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subject_creation() {
        let subject = ConsensusSubject::new("orders.new");
        assert_eq!(subject.name(), "orders.new");
    }

    #[test]
    fn test_exact_match() {
        assert!(subject_matches_pattern("orders.new", "orders.new"));
        assert!(!subject_matches_pattern("orders.new", "orders.old"));
    }

    #[test]
    fn test_single_wildcard() {
        assert!(subject_matches_pattern("orders.new", "orders.*"));
        assert!(subject_matches_pattern("orders.old", "orders.*"));
        assert!(subject_matches_pattern("users.123", "users.*"));
        assert!(!subject_matches_pattern("orders.new.item", "orders.*"));
        assert!(!subject_matches_pattern("orders", "orders.*"));
    }

    #[test]
    fn test_multi_wildcard() {
        assert!(subject_matches_pattern("orders.new", "orders.>"));
        assert!(subject_matches_pattern("orders.new.item", "orders.>"));
        assert!(subject_matches_pattern(
            "orders.new.item.urgent",
            "orders.>"
        ));
        assert!(!subject_matches_pattern("users.new", "orders.>"));
        assert!(!subject_matches_pattern("orders", "orders.>"));
    }

    #[test]
    fn test_combined_wildcards() {
        assert!(subject_matches_pattern("orders.us.new", "orders.*.>"));
        assert!(subject_matches_pattern("orders.us.new.item", "orders.*.>"));
        assert!(!subject_matches_pattern("orders.new", "orders.*.>"));
        assert!(!subject_matches_pattern("users.us.new", "orders.*.>"));
    }

    #[test]
    fn test_router_basic() {
        let mut router = SubjectRouter::new();

        router.subscribe_stream("stream1", "orders.*");
        router.subscribe_stream("stream2", "orders.new");
        router.subscribe_stream("stream1", "users.>");

        let streams = router.route_subject("orders.new");
        assert!(streams.contains("stream1"));
        assert!(streams.contains("stream2"));

        let streams = router.route_subject("orders.old");
        assert!(streams.contains("stream1"));
        assert!(!streams.contains("stream2"));

        let streams = router.route_subject("users.123.profile");
        assert!(streams.contains("stream1"));
        assert!(!streams.contains("stream2"));
    }

    #[test]
    fn test_router_unsubscribe() {
        let mut router = SubjectRouter::new();

        router.subscribe_stream("stream1", "orders.*");
        router.subscribe_stream("stream2", "orders.*");

        let streams = router.route_subject("orders.new");
        assert_eq!(streams.len(), 2);

        router.unsubscribe_stream("stream1", "orders.*");
        let streams = router.route_subject("orders.new");
        assert_eq!(streams.len(), 1);
        assert!(streams.contains("stream2"));
    }

    #[test]
    fn test_router_remove_stream() {
        let mut router = SubjectRouter::new();

        router.subscribe_stream("stream1", "orders.*");
        router.subscribe_stream("stream1", "users.>");

        let streams = router.route_subject("orders.new");
        assert!(streams.contains("stream1"));

        router.remove_stream("stream1");
        let streams = router.route_subject("orders.new");
        assert!(streams.is_empty());
    }

    #[test]
    fn test_subject_routing_workflow() {
        let mut router = SubjectRouter::new();

        // Set up subscriptions
        router.subscribe_stream("orders_stream", "orders.*");
        router.subscribe_stream("all_orders_stream", "orders.>");
        router.subscribe_stream("new_orders_stream", "orders.new");
        router.subscribe_stream("users_stream", "users.*");

        // Test routing to a specific subject
        let streams = router.route_subject("orders.new");
        assert_eq!(streams.len(), 3); // orders.*, orders.>, orders.new should all match
        assert!(streams.contains("orders_stream"));
        assert!(streams.contains("all_orders_stream"));
        assert!(streams.contains("new_orders_stream"));

        // Test routing to another orders subject
        let streams = router.route_subject("orders.cancelled");
        assert_eq!(streams.len(), 2); // orders.*, orders.> should match
        assert!(streams.contains("orders_stream"));
        assert!(streams.contains("all_orders_stream"));
        assert!(!streams.contains("new_orders_stream"));

        // Test routing to hierarchical subject
        let streams = router.route_subject("orders.new.urgent");
        assert_eq!(streams.len(), 1); // only orders.> should match
        assert!(streams.contains("all_orders_stream"));
        assert!(!streams.contains("orders_stream")); // orders.* matches only 2 tokens
        assert!(!streams.contains("new_orders_stream")); // exact match only

        // Test routing to users subject
        let streams = router.route_subject("users.123");
        assert_eq!(streams.len(), 1);
        assert!(streams.contains("users_stream"));

        // Test routing to non-matching subject
        let streams = router.route_subject("products.new");
        assert!(streams.is_empty());
    }
}
