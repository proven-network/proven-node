//! Logger context provider for TUI-specific logging
//!
//! This module provides adapters between TUI types and proven-logger context.

use crate::messages::TuiNodeId;
use proven_logger::Context;
use std::borrow::Cow;

/// Convert a `TuiNodeId` to a logger context
#[must_use]
pub fn node_context(node_id: TuiNodeId) -> Context {
    Context::new("tui-node").with_node_id(Cow::Owned(format!("node-{node_id}")))
}

/// Create a context for the main TUI thread
#[must_use]
pub fn main_context() -> Context {
    Context::new("tui-main").with_node_id(Cow::Borrowed("main"))
}

/// Create a context for a specific component
#[must_use]
pub const fn component_context(component: &'static str) -> Context {
    Context::new(component)
}

/// Extension trait for adding TUI-specific context to loggers
pub trait TuiLoggerExt {
    /// Create a logger with node context
    fn with_node(&self, node_id: TuiNodeId) -> std::sync::Arc<dyn proven_logger::Logger>;

    /// Create a logger for the main thread
    fn with_main(&self) -> std::sync::Arc<dyn proven_logger::Logger>;
}

impl<T: proven_logger::Logger + ?Sized> TuiLoggerExt for T {
    fn with_node(&self, node_id: TuiNodeId) -> std::sync::Arc<dyn proven_logger::Logger> {
        self.with_context(node_context(node_id))
    }

    fn with_main(&self) -> std::sync::Arc<dyn proven_logger::Logger> {
        self.with_context(main_context())
    }
}

// Thread-local logger context for implicit context propagation
thread_local! {
    static CURRENT_CONTEXT: std::cell::RefCell<Option<Context>> = const { std::cell::RefCell::new(None) };
}

/// Set the current thread's logging context
pub fn set_thread_context(context: Context) {
    CURRENT_CONTEXT.with(|c| {
        *c.borrow_mut() = Some(context);
    });
}

/// Clear the current thread's logging context
pub fn clear_thread_context() {
    CURRENT_CONTEXT.with(|c| {
        *c.borrow_mut() = None;
    });
}

/// Get the current thread's logging context
#[must_use]
pub fn get_thread_context() -> Option<Context> {
    CURRENT_CONTEXT.with(|c| c.borrow().clone())
}

/// RAII guard for temporarily setting thread context
pub struct ContextGuard {
    previous: Option<Context>,
}

impl ContextGuard {
    /// Create a new context guard
    #[must_use]
    pub fn new(context: Context) -> Self {
        let previous = get_thread_context();
        set_thread_context(context);
        Self { previous }
    }
}

impl Drop for ContextGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(ctx) => set_thread_context(ctx.clone()),
            None => clear_thread_context(),
        }
    }
}

/// Macro for logging with implicit context from thread-local storage
#[macro_export]
macro_rules! log_with_context {
    ($level:expr, $($arg:tt)*) => {{
        if let Some(logger) = proven_logger::get() {
            if let Some(context) = $crate::logger_context::get_thread_context() {
                // Create a temporary logger with the context
                let context_logger = logger.with_context(context);
                match $level {
                    proven_logger::Level::Error => proven_logger::LoggerExt::error(&*context_logger, format!($($arg)*)),
                    proven_logger::Level::Warn => proven_logger::LoggerExt::warn(&*context_logger, format!($($arg)*)),
                    proven_logger::Level::Info => proven_logger::LoggerExt::info(&*context_logger, format!($($arg)*)),
                    proven_logger::Level::Debug => proven_logger::LoggerExt::debug(&*context_logger, format!($($arg)*)),
                    proven_logger::Level::Trace => proven_logger::LoggerExt::trace(&*context_logger, format!($($arg)*)),
                }
            } else {
                // Fall back to regular logging
                match $level {
                    proven_logger::Level::Error => proven_logger::error!($($arg)*),
                    proven_logger::Level::Warn => proven_logger::warn!($($arg)*),
                    proven_logger::Level::Info => proven_logger::info!($($arg)*),
                    proven_logger::Level::Debug => proven_logger::debug!($($arg)*),
                    proven_logger::Level::Trace => proven_logger::trace!($($arg)*),
                }
            }
        }
    }};
}

/// Convenience macros for different log levels
#[macro_export]
macro_rules! error_with_context {
    ($($arg:tt)*) => {
        $crate::log_with_context!(proven_logger::Level::Error, $($arg)*)
    };
}

/// Convenience macro for warning with context
#[macro_export]
macro_rules! warn_with_context {
    ($($arg:tt)*) => {
        $crate::log_with_context!(proven_logger::Level::Warn, $($arg)*)
    };
}

/// Convenience macro for info with context
#[macro_export]
macro_rules! info_with_context {
    ($($arg:tt)*) => {
        $crate::log_with_context!(proven_logger::Level::Info, $($arg)*)
    };
}

/// Convenience macro for debug with context
#[macro_export]
macro_rules! debug_with_context {
    ($($arg:tt)*) => {
        $crate::log_with_context!(proven_logger::Level::Debug, $($arg)*)
    };
}

/// Convenience macro for trace with context
#[macro_export]
macro_rules! trace_with_context {
    ($($arg:tt)*) => {
        $crate::log_with_context!(proven_logger::Level::Trace, $($arg)*)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_context() {
        let node_id = TuiNodeId::with_values(1, 25);
        let context = node_context(node_id);

        assert_eq!(context.component, "tui-node");
        assert_eq!(context.node_id.as_deref(), Some("node-1-25"));
    }

    #[test]
    fn test_main_context() {
        let context = main_context();

        assert_eq!(context.component, "tui-main");
        assert_eq!(context.node_id.as_deref(), Some("main"));
    }

    #[test]
    fn test_context_guard() {
        // Initially no context
        assert!(get_thread_context().is_none());

        {
            let _guard = ContextGuard::new(main_context());
            // Context is set
            let ctx = get_thread_context().unwrap();
            assert_eq!(ctx.node_id.as_deref(), Some("main"));
        }

        // Context is cleared after guard drops
        assert!(get_thread_context().is_none());
    }
}
