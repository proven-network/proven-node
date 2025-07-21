//! Core logger trait

use crate::{Context, Level, Record};
use std::sync::Arc;

/// Core logger trait - designed for performance
pub trait Logger: Send + Sync + 'static {
    /// Log a record
    fn log(&self, record: Record);

    /// Flush any buffered logs
    fn flush(&self);

    /// Check if a level is enabled (for fast filtering)
    #[inline(always)]
    fn is_enabled(&self, level: Level) -> bool {
        // Default implementation - always check against static max
        level.is_enabled_static()
    }

    /// Create a child logger with additional context
    fn with_context(&self, context: Context) -> Arc<dyn Logger>;
}

/// Extension trait for convenient logging methods
pub trait LoggerExt: Logger {
    /// Log an error
    #[inline(always)]
    fn error(&self, msg: impl Into<std::borrow::Cow<'static, str>>) {
        // Compile-time check first
        if Level::Error.is_enabled_static() && self.is_enabled(Level::Error) {
            self.log(Record::new(Level::Error, msg));
        }
    }

    /// Log a warning
    #[inline(always)]
    fn warn(&self, msg: impl Into<std::borrow::Cow<'static, str>>) {
        // Compile-time check first
        if Level::Warn.is_enabled_static() && self.is_enabled(Level::Warn) {
            self.log(Record::new(Level::Warn, msg));
        }
    }

    /// Log info
    #[inline(always)]
    fn info(&self, msg: impl Into<std::borrow::Cow<'static, str>>) {
        // Compile-time check first
        if Level::Info.is_enabled_static() && self.is_enabled(Level::Info) {
            self.log(Record::new(Level::Info, msg));
        }
    }

    /// Log debug
    #[inline(always)]
    fn debug(&self, msg: impl Into<std::borrow::Cow<'static, str>>) {
        // Compile-time check first
        if Level::Debug.is_enabled_static() && self.is_enabled(Level::Debug) {
            self.log(Record::new(Level::Debug, msg));
        }
    }

    /// Log trace
    #[inline(always)]
    fn trace(&self, msg: impl Into<std::borrow::Cow<'static, str>>) {
        // Compile-time check first
        if Level::Trace.is_enabled_static() && self.is_enabled(Level::Trace) {
            self.log(Record::new(Level::Trace, msg));
        }
    }
}

// Implement for all loggers
impl<T: Logger + ?Sized> LoggerExt for T {}
