//! Log record type optimized for performance

use crate::{Context, Level};
#[cfg(feature = "timestamps")]
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// A log record - designed to minimize allocations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record<'a> {
    /// Log level
    pub level: Level,
    /// The log message
    pub message: Cow<'a, str>,
    /// When the log was created (can be disabled for performance)
    #[cfg(feature = "timestamps")]
    pub timestamp: DateTime<Utc>,
    /// Target module
    pub target: &'static str,
    /// File location
    pub file: Option<&'static str>,
    /// Line number
    pub line: Option<u32>,
    /// Context information
    #[serde(skip)]
    pub context: Option<&'a Context>,
}

impl<'a> Record<'a> {
    /// Create a new record with minimal allocations
    #[inline]
    pub fn new(level: Level, message: impl Into<Cow<'a, str>>) -> Self {
        Self {
            level,
            message: message.into(),
            #[cfg(feature = "timestamps")]
            timestamp: Utc::now(),
            target: module_path!(),
            file: None,
            line: None,
            context: None,
        }
    }

    /// Builder-style method for setting target
    #[inline]
    pub fn with_target(mut self, target: &'static str) -> Self {
        self.target = target;
        self
    }

    /// Builder-style method for setting location
    #[inline]
    pub fn with_location(mut self, file: &'static str, line: u32) -> Self {
        self.file = Some(file);
        self.line = Some(line);
        self
    }

    /// Builder-style method for setting context
    #[inline]
    pub fn with_context(mut self, context: &'a Context) -> Self {
        self.context = Some(context);
        self
    }

    /// Convert to owned version (for sending across threads)
    pub fn to_owned(self) -> OwnedRecord {
        OwnedRecord {
            level: self.level,
            message: self.message.into_owned(),
            #[cfg(feature = "timestamps")]
            timestamp: self.timestamp,
            target: self.target,
            file: self.file,
            line: self.line,
            context: self.context.cloned(),
        }
    }
}

/// Owned version of Record for cross-thread sending
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OwnedRecord {
    /// Log level
    pub level: Level,
    /// The log message
    pub message: String,
    /// When the log was created
    #[cfg(feature = "timestamps")]
    pub timestamp: DateTime<Utc>,
    /// Target module
    pub target: &'static str,
    /// File location
    pub file: Option<&'static str>,
    /// Line number
    pub line: Option<u32>,
    /// Context information
    pub context: Option<Context>,
}
