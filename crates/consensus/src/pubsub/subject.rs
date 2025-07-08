//! Subject routing and pattern matching for consensus messaging

use std::collections::{HashMap, HashSet};

use crate::error::{ConsensusResult, Error};

/// Validates a literal subject (no wildcards allowed)
pub fn validate_subject(subject: &str) -> ConsensusResult<()> {
    if subject.is_empty() {
        return Err(Error::InvalidSubjectPattern(
            "Subject cannot be empty".to_string(),
        ));
    }

    if subject.contains(['*', '>']) {
        return Err(Error::InvalidSubjectPattern(
            "Subject cannot contain wildcards (use for patterns only)".to_string(),
        ));
    }

    let tokens: Vec<&str> = subject.split('.').collect();
    for token in tokens {
        if token.is_empty() {
            return Err(Error::InvalidSubjectPattern(
                "Subject token cannot be empty".to_string(),
            ));
        }
    }

    Ok(())
}

/// Validates a subject pattern
pub fn validate_subject_pattern(pattern: &str) -> ConsensusResult<()> {
    if pattern.is_empty() {
        return Err(Error::InvalidSubjectPattern(
            "Subject pattern cannot be empty".to_string(),
        ));
    }

    let tokens: Vec<&str> = pattern.split('.').collect();
    let mut found_gt = false;

    for (i, token) in tokens.iter().enumerate() {
        if token.is_empty() {
            return Err(Error::InvalidSubjectPattern(
                "Subject token cannot be empty".to_string(),
            ));
        }

        if *token == ">" {
            if found_gt {
                return Err(Error::InvalidSubjectPattern(
                    "Only one '>' wildcard allowed".to_string(),
                ));
            }
            if i != tokens.len() - 1 {
                return Err(Error::InvalidSubjectPattern(
                    "'>' wildcard must be at the end".to_string(),
                ));
            }
            found_gt = true;
        }
    }

    Ok(())
}

/// Check if a subject matches a pattern with wildcard support
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

/// Router for mapping subjects to streams based on subscription patterns
#[derive(Debug, Clone, Default)]
pub struct SubjectRouter {
    /// Map of stream names to their subscribed subject patterns
    stream_subscriptions: HashMap<String, HashSet<String>>,
    /// Reverse index: subject pattern to streams
    pattern_to_streams: HashMap<String, HashSet<String>>,
}

impl SubjectRouter {
    /// Create a new subject router
    pub fn new() -> Self {
        Self::default()
    }

    /// Subscribe a stream to a subject pattern
    pub fn subscribe_stream(&mut self, stream_name: &str, subject_pattern: &str) {
        self.stream_subscriptions
            .entry(stream_name.to_string())
            .or_default()
            .insert(subject_pattern.to_string());

        self.pattern_to_streams
            .entry(subject_pattern.to_string())
            .or_default()
            .insert(stream_name.to_string());
    }

    /// Unsubscribe a stream from a subject pattern
    pub fn unsubscribe_stream(&mut self, stream_name: &str, subject_pattern: &str) {
        if let Some(patterns) = self.stream_subscriptions.get_mut(stream_name) {
            patterns.remove(subject_pattern);
            if patterns.is_empty() {
                self.stream_subscriptions.remove(stream_name);
            }
        }

        if let Some(streams) = self.pattern_to_streams.get_mut(subject_pattern) {
            streams.remove(stream_name);
            if streams.is_empty() {
                self.pattern_to_streams.remove(subject_pattern);
            }
        }
    }

    /// Remove all subscriptions for a stream
    pub fn remove_stream(&mut self, stream_name: &str) {
        if let Some(patterns) = self.stream_subscriptions.remove(stream_name) {
            for pattern in patterns {
                if let Some(streams) = self.pattern_to_streams.get_mut(&pattern) {
                    streams.remove(stream_name);
                    if streams.is_empty() {
                        self.pattern_to_streams.remove(&pattern);
                    }
                }
            }
        }
    }

    /// Route a subject to all matching streams
    pub fn route_subject(&self, subject: &str) -> HashSet<String> {
        let mut matching_streams = HashSet::new();

        for (pattern, streams) in &self.pattern_to_streams {
            if subject_matches_pattern(subject, pattern) {
                matching_streams.extend(streams.iter().cloned());
            }
        }

        matching_streams
    }

    /// Get all subject patterns that a stream is subscribed to
    pub fn get_stream_subjects(&self, stream_name: &str) -> Option<&HashSet<String>> {
        self.stream_subscriptions.get(stream_name)
    }

    /// Get all subscriptions (pattern to streams mapping)
    pub fn get_subscriptions(&self) -> &HashMap<String, HashSet<String>> {
        &self.pattern_to_streams
    }

    /// Clear all subscriptions
    pub fn clear(&mut self) {
        self.stream_subscriptions.clear();
        self.pattern_to_streams.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subject_matches_pattern() {
        // Exact matches
        assert!(subject_matches_pattern("foo.bar", "foo.bar"));
        assert!(!subject_matches_pattern("foo.bar", "foo.baz"));

        // Single wildcard
        assert!(subject_matches_pattern("foo.bar", "foo.*"));
        assert!(subject_matches_pattern("foo.bar", "*.bar"));
        assert!(subject_matches_pattern("foo.bar", "*.*"));
        assert!(!subject_matches_pattern("foo.bar.baz", "foo.*"));

        // Multi wildcard
        assert!(subject_matches_pattern("foo.bar", "foo.>"));
        assert!(subject_matches_pattern("foo.bar.baz", "foo.>"));
        assert!(subject_matches_pattern("foo.bar.baz.qux", "foo.>"));
        assert!(subject_matches_pattern("foo", ">"));

        // Complex patterns
        assert!(subject_matches_pattern("foo.bar.baz", "foo.*.baz"));
        assert!(subject_matches_pattern("foo.bar.baz", "*.bar.*"));
        assert!(!subject_matches_pattern("foo.bar", "foo.bar.baz"));
    }

    #[test]
    fn test_validate_subject() {
        // Valid literal subjects
        assert!(validate_subject("foo.bar").is_ok());
        assert!(validate_subject("foo.bar.baz").is_ok());
        assert!(validate_subject("events.user.login").is_ok());
        assert!(validate_subject("a").is_ok());

        // Invalid subjects
        assert!(validate_subject("").is_err()); // Empty
        assert!(validate_subject("foo..bar").is_err()); // Empty token
        assert!(validate_subject("foo.*").is_err()); // Contains wildcard
        assert!(validate_subject("foo.>").is_err()); // Contains wildcard
        assert!(validate_subject("*.bar").is_err()); // Contains wildcard
        assert!(validate_subject("foo.bar.").is_err()); // Trailing dot
        assert!(validate_subject(".foo.bar").is_err()); // Leading dot
    }

    #[test]
    fn test_validate_subject_pattern() {
        assert!(validate_subject_pattern("foo.bar").is_ok());
        assert!(validate_subject_pattern("foo.*").is_ok());
        assert!(validate_subject_pattern("foo.>").is_ok());
        assert!(validate_subject_pattern("*.*.*").is_ok());

        assert!(validate_subject_pattern("").is_err());
        assert!(validate_subject_pattern("foo..bar").is_err());
        assert!(validate_subject_pattern("foo.>.bar").is_err());
        assert!(validate_subject_pattern("foo.>.>").is_err());
    }

    #[test]
    fn test_subject_router() {
        let mut router = SubjectRouter::new();

        router.subscribe_stream("stream1", "foo.*");
        router.subscribe_stream("stream1", "bar.>");
        router.subscribe_stream("stream2", "foo.bar");
        router.subscribe_stream("stream2", "*.baz");

        let matches = router.route_subject("foo.bar");
        assert!(matches.contains("stream1"));
        assert!(matches.contains("stream2"));

        let matches = router.route_subject("bar.baz.qux");
        assert!(matches.contains("stream1"));
        assert!(!matches.contains("stream2"));

        let matches = router.route_subject("hello.baz");
        assert!(!matches.contains("stream1"));
        assert!(matches.contains("stream2"));

        router.unsubscribe_stream("stream1", "foo.*");
        let matches = router.route_subject("foo.bar");
        assert!(!matches.contains("stream1"));
        assert!(matches.contains("stream2"));
    }

    #[test]
    fn test_subject_router_remove_stream() {
        let mut router = SubjectRouter::new();

        router.subscribe_stream("stream1", "foo.*");
        router.subscribe_stream("stream1", "bar.>");
        router.subscribe_stream("stream2", "foo.*");

        // Before removal
        let matches = router.route_subject("foo.test");
        assert_eq!(matches.len(), 2);

        // Remove stream1
        router.remove_stream("stream1");

        // After removal
        let matches = router.route_subject("foo.test");
        assert_eq!(matches.len(), 1);
        assert!(matches.contains("stream2"));

        // bar.> pattern should be completely removed
        let matches = router.route_subject("bar.something");
        assert_eq!(matches.len(), 0);
    }
}
