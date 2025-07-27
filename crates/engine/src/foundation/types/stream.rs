//! Stream-related types

use serde::{Deserialize, Serialize};
use std::fmt;

/// Stream name type
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamName(String);

impl StreamName {
    /// Create a new stream name
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Get the name as a string slice
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StreamName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for StreamName {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for StreamName {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}
