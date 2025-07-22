//! High-performance logging framework for Proven Network
//!
//! This crate provides:
//! - Familiar macro interface similar to `tracing`
//! - Instance-based loggers with context
//! - Zero-cost when disabled via compile-time features
//! - Lock-free logging path for performance
//! - Support for vsock, file, and test backends

#![warn(missing_docs, unreachable_pub)]
#![forbid(unsafe_code)]

use std::sync::Arc;

mod context;
mod level;
mod logger;
mod macros;
mod record;

pub use context::*;
pub use level::*;
pub use logger::*;
pub use record::*;

// Re-export for macros
#[doc(hidden)]
pub use once_cell::sync::OnceCell;

/// Global logger instance
static LOGGER: OnceCell<Arc<dyn Logger>> = OnceCell::new();

/// Initialize the global logger
///
/// This should be called once at application startup.
/// Subsequent calls will return an error.
#[inline]
pub fn init(logger: Arc<dyn Logger>) -> Result<(), Box<dyn std::error::Error>> {
    LOGGER
        .set(logger)
        .map_err(|_| "Logger already initialized".into())
}

/// Get the global logger if initialized
#[inline(always)]
pub fn get() -> Option<&'static Arc<dyn Logger>> {
    LOGGER.get()
}

/// Try to get the global logger (for macros)
#[doc(hidden)]
#[inline(always)]
pub fn __private_try_get() -> Option<Arc<dyn Logger>> {
    LOGGER.get().cloned()
}

// Default implementations
#[cfg(feature = "stdout")]
mod stdout;
#[cfg(feature = "stdout")]
pub use stdout::StdoutLogger;

// Test support
#[cfg(feature = "test-support")]
pub mod test_support;

// Re-export test macros at crate root for convenience
#[cfg(feature = "test-support")]
pub use test_support::{logged_test, logged_tokio_test};

// Compatibility bridges
#[cfg(any(feature = "log-compat", feature = "tracing-compat"))]
pub mod compat;

/// A no-op logger that discards all logs
#[derive(Debug, Clone)]
pub struct NoOpLogger;

impl Logger for NoOpLogger {
    #[inline(always)]
    fn log(&self, _record: Record) {}

    #[inline(always)]
    fn flush(&self) {}

    #[inline(always)]
    fn is_enabled(&self, _level: Level) -> bool {
        false
    }

    fn with_context(&self, _ctx: Context) -> Arc<dyn Logger> {
        Arc::new(NoOpLogger)
    }
}
