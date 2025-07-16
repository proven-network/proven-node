//! Subject validation and pattern matching

use super::types::{PubSubError, PubSubResult};
use std::fmt;

/// A validated subject (no wildcards)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Subject(String);

impl Subject {
    /// Create a new subject after validation
    pub fn new(subject: impl Into<String>) -> PubSubResult<Self> {
        let subject = subject.into();
        validate_subject(&subject)?;
        Ok(Self(subject))
    }

    /// Get the subject as a string slice
    pub fn as_str(&self) -> &str {
        &self.0
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

/// A subject pattern (can contain wildcards)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubjectPattern(String);

impl SubjectPattern {
    /// Create a new subject pattern after validation
    pub fn new(pattern: impl Into<String>) -> PubSubResult<Self> {
        let pattern = pattern.into();
        validate_subject_pattern(&pattern)?;
        Ok(Self(pattern))
    }

    /// Get the pattern as a string slice
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Check if this pattern matches a subject
    pub fn matches(&self, subject: &str) -> bool {
        subject_matches_pattern(subject, &self.0)
    }
}

impl fmt::Display for SubjectPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Validates a literal subject (no wildcards allowed)
pub fn validate_subject(subject: &str) -> PubSubResult<()> {
    if subject.is_empty() {
        return Err(PubSubError::InvalidSubject(
            "Subject cannot be empty".to_string(),
        ));
    }

    if subject.contains(['*', '>']) {
        return Err(PubSubError::InvalidSubject(
            "Subject cannot contain wildcards (use patterns for wildcards)".to_string(),
        ));
    }

    let tokens: Vec<&str> = subject.split('.').collect();
    for token in tokens {
        if token.is_empty() {
            return Err(PubSubError::InvalidSubject(
                "Subject token cannot be empty".to_string(),
            ));
        }
    }

    Ok(())
}

/// Validates a subject pattern
pub fn validate_subject_pattern(pattern: &str) -> PubSubResult<()> {
    if pattern.is_empty() {
        return Err(PubSubError::InvalidSubject(
            "Subject pattern cannot be empty".to_string(),
        ));
    }

    let tokens: Vec<&str> = pattern.split('.').collect();
    let mut found_gt = false;

    for (i, token) in tokens.iter().enumerate() {
        if token.is_empty() {
            return Err(PubSubError::InvalidSubject(
                "Subject token cannot be empty".to_string(),
            ));
        }

        if *token == ">" {
            if found_gt {
                return Err(PubSubError::InvalidSubject(
                    "Only one '>' wildcard allowed".to_string(),
                ));
            }
            if i != tokens.len() - 1 {
                return Err(PubSubError::InvalidSubject(
                    "'>' wildcard must be at the end".to_string(),
                ));
            }
            found_gt = true;
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

        // Invalid subjects
        assert!(Subject::new("").is_err());
        assert!(Subject::new("metrics.*").is_err());
        assert!(Subject::new("events.>").is_err());
        assert!(Subject::new("foo..bar").is_err());
    }

    #[test]
    fn test_pattern_validation() {
        // Valid patterns
        assert!(SubjectPattern::new("metrics.*").is_ok());
        assert!(SubjectPattern::new("events.>").is_ok());
        assert!(SubjectPattern::new("*.user.*").is_ok());

        // Invalid patterns
        assert!(SubjectPattern::new("").is_err());
        assert!(SubjectPattern::new("foo..bar").is_err());
        assert!(SubjectPattern::new("foo.>.bar").is_err());
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
    }
}
