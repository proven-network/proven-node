//! Subject types for PubSub messaging
//!
//! This module provides validated subject types that ensure proper formatting
//! and support for wildcard pattern matching.

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Errors that can occur with subject validation
#[derive(Error, Debug)]
pub enum SubjectError {
    /// Subject is empty
    #[error("Subject cannot be empty")]
    Empty,

    /// Subject contains invalid characters
    #[error("Subject contains invalid characters: {0}")]
    InvalidCharacters(String),

    /// Subject contains wildcards where they're not allowed
    #[error("Subject cannot contain wildcards: {0}")]
    ContainsWildcards(String),

    /// Invalid token in subject
    #[error("Invalid token in subject: {0}")]
    InvalidToken(String),

    /// Invalid wildcard usage
    #[error("Invalid wildcard usage: {0}")]
    InvalidWildcard(String),
}

/// A validated subject (no wildcards)
///
/// Subjects are dot-separated hierarchical names used for routing messages.
/// Examples: "metrics.cpu", "events.user.login", "stream.data.append"
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Subject(String);

impl Subject {
    /// Create a new subject after validation
    pub fn new(subject: impl Into<String>) -> Result<Self, SubjectError> {
        let subject = subject.into();
        validate_subject(&subject)?;
        Ok(Self(subject))
    }

    /// Create a subject without validation (unsafe)
    ///
    /// # Safety
    /// This bypasses validation and should only be used when the subject
    /// is known to be valid (e.g., from trusted internal sources)
    pub unsafe fn new_unchecked(subject: String) -> Self {
        Self(subject)
    }

    /// Get the subject as a string slice
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Convert to owned string
    pub fn into_string(self) -> String {
        self.0
    }

    /// Get tokens
    pub fn tokens(&self) -> Vec<&str> {
        self.0.split('.').collect()
    }
}

impl fmt::Display for Subject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for Subject {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// A subject pattern (can contain wildcards)
///
/// Patterns support wildcards for flexible matching:
/// - `*` matches exactly one token
/// - `>` matches zero or more tokens (must be at the end)
///
/// Examples: "metrics.*", "events.>", "*.user.*"
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SubjectPattern(String);

impl SubjectPattern {
    /// Create a new subject pattern after validation
    pub fn new(pattern: impl Into<String>) -> Result<Self, SubjectError> {
        let pattern = pattern.into();
        validate_subject_pattern(&pattern)?;
        Ok(Self(pattern))
    }

    /// Create a pattern without validation (unsafe)
    ///
    /// # Safety
    /// This bypasses validation and should only be used when the pattern
    /// is known to be valid (e.g., from trusted internal sources)
    pub unsafe fn new_unchecked(pattern: String) -> Self {
        Self(pattern)
    }

    /// Get the pattern as a string slice
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Convert to owned string
    pub fn into_string(self) -> String {
        self.0
    }

    /// Check if this pattern matches a subject
    pub fn matches(&self, subject: &str) -> bool {
        subject_matches_pattern(subject, &self.0)
    }

    /// Check if this pattern matches a Subject
    pub fn matches_subject(&self, subject: &Subject) -> bool {
        self.matches(subject.as_str())
    }
}

impl fmt::Display for SubjectPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for SubjectPattern {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Validates a literal subject (no wildcards allowed)
fn validate_subject(subject: &str) -> Result<(), SubjectError> {
    if subject.is_empty() {
        return Err(SubjectError::Empty);
    }

    if subject.contains(['*', '>']) {
        return Err(SubjectError::ContainsWildcards(
            "Use SubjectPattern for wildcards".to_string(),
        ));
    }

    let tokens: Vec<&str> = subject.split('.').collect();
    for token in tokens {
        if token.is_empty() {
            return Err(SubjectError::InvalidToken(
                "Empty token between dots".to_string(),
            ));
        }

        // Additional validation for allowed characters
        if !token
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
        {
            return Err(SubjectError::InvalidCharacters(format!(
                "Token '{token}' contains invalid characters"
            )));
        }
    }

    Ok(())
}

/// Validates a subject pattern
fn validate_subject_pattern(pattern: &str) -> Result<(), SubjectError> {
    if pattern.is_empty() {
        return Err(SubjectError::Empty);
    }

    let tokens: Vec<&str> = pattern.split('.').collect();
    let mut found_gt = false;

    for (i, token) in tokens.iter().enumerate() {
        if token.is_empty() {
            return Err(SubjectError::InvalidToken(
                "Empty token between dots".to_string(),
            ));
        }

        match *token {
            ">" => {
                if found_gt {
                    return Err(SubjectError::InvalidWildcard(
                        "Only one '>' wildcard allowed".to_string(),
                    ));
                }
                if i != tokens.len() - 1 {
                    return Err(SubjectError::InvalidWildcard(
                        "'>' wildcard must be at the end".to_string(),
                    ));
                }
                found_gt = true;
            }
            "*" => {
                // Single token wildcard is valid anywhere
            }
            _ => {
                // Literal token - validate characters
                if !token
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
                {
                    return Err(SubjectError::InvalidCharacters(format!(
                        "Token '{token}' contains invalid characters"
                    )));
                }
            }
        }
    }

    Ok(())
}

/// Check if a subject matches a pattern with wildcard support
///
/// Wildcards:
/// - `*` matches exactly one token
/// - `>` matches zero or more tokens (must be at the end)
pub fn subject_matches_pattern(subject: &str, pattern: &str) -> bool {
    let subject_tokens: Vec<&str> = subject.split('.').collect();
    let pattern_tokens: Vec<&str> = pattern.split('.').collect();

    let mut si = 0;
    let mut pi = 0;

    while si < subject_tokens.len() && pi < pattern_tokens.len() {
        let pattern_token = pattern_tokens[pi];

        match pattern_token {
            "*" => {
                // Single token wildcard
                si += 1;
                pi += 1;
            }
            ">" => {
                // Multi-token wildcard, matches rest of subject
                return true;
            }
            _ => {
                // Literal match
                if subject_tokens[si] != pattern_token {
                    return false;
                }
                si += 1;
                pi += 1;
            }
        }
    }

    // Check if we've consumed both subject and pattern
    si == subject_tokens.len() && pi == pattern_tokens.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subject_validation() {
        // Valid subjects
        assert!(Subject::new("metrics.cpu").is_ok());
        assert!(Subject::new("events.user.login").is_ok());
        assert!(Subject::new("stream.data.append").is_ok());
        assert!(Subject::new("test_subject").is_ok());
        assert!(Subject::new("test-subject").is_ok());

        // Invalid subjects
        assert!(matches!(Subject::new(""), Err(SubjectError::Empty)));
        assert!(matches!(
            Subject::new("metrics.*"),
            Err(SubjectError::ContainsWildcards(_))
        ));
        assert!(matches!(
            Subject::new("events.>"),
            Err(SubjectError::ContainsWildcards(_))
        ));
        assert!(matches!(
            Subject::new("foo..bar"),
            Err(SubjectError::InvalidToken(_))
        ));
        assert!(matches!(
            Subject::new("foo.bar$"),
            Err(SubjectError::InvalidCharacters(_))
        ));
    }

    #[test]
    fn test_pattern_validation() {
        // Valid patterns
        assert!(SubjectPattern::new("metrics.*").is_ok());
        assert!(SubjectPattern::new("events.>").is_ok());
        assert!(SubjectPattern::new("*.user.*").is_ok());
        assert!(SubjectPattern::new("test_pattern.*").is_ok());
        assert!(SubjectPattern::new("test-pattern.>").is_ok());

        // Invalid patterns
        assert!(matches!(SubjectPattern::new(""), Err(SubjectError::Empty)));
        assert!(matches!(
            SubjectPattern::new("foo..bar"),
            Err(SubjectError::InvalidToken(_))
        ));
        assert!(matches!(
            SubjectPattern::new("foo.>.bar"),
            Err(SubjectError::InvalidWildcard(_))
        ));
        assert!(matches!(
            SubjectPattern::new("foo.bar$"),
            Err(SubjectError::InvalidCharacters(_))
        ));
    }

    #[test]
    fn test_pattern_matching() {
        // Exact matches
        assert!(subject_matches_pattern("foo.bar", "foo.bar"));
        assert!(!subject_matches_pattern("foo.bar", "foo.baz"));

        // Single wildcard
        assert!(subject_matches_pattern("foo.bar", "foo.*"));
        assert!(subject_matches_pattern("foo.bar", "*.bar"));
        assert!(!subject_matches_pattern("foo.bar.baz", "foo.*"));

        // Multi wildcard
        assert!(subject_matches_pattern("foo.bar", "foo.>"));
        assert!(subject_matches_pattern("foo.bar.baz", "foo.>"));
        assert!(subject_matches_pattern("foo", ">"));
        assert!(subject_matches_pattern("foo.bar.baz.qux", "foo.>"));

        // Edge cases
        assert!(!subject_matches_pattern("foo.bar", "foo.bar.>"));
        assert!(subject_matches_pattern("foo.bar", ">"));
    }

    #[test]
    fn test_subject_pattern_methods() {
        let pattern = SubjectPattern::new("foo.*").unwrap();
        let subject = Subject::new("foo.bar").unwrap();

        assert!(pattern.matches("foo.bar"));
        assert!(pattern.matches_subject(&subject));
        assert!(!pattern.matches("foo.bar.baz"));
    }
}
