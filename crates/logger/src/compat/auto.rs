//! Automatic bridging utilities

use crate::Logger;
use std::sync::Arc;

/// Error type for bridge initialization
#[derive(Debug)]
pub enum BridgeError {
    /// Failed to set log bridge
    #[cfg(feature = "log-compat")]
    LogBridge(log::SetLoggerError),
    /// Failed to set tracing bridge
    #[cfg(feature = "tracing-compat")]
    TracingBridge(Box<dyn std::error::Error>),
    /// No bridges available
    NoBridges,
}

impl std::fmt::Display for BridgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "log-compat")]
            BridgeError::LogBridge(e) => write!(f, "Failed to initialize log bridge: {e}"),
            #[cfg(feature = "tracing-compat")]
            BridgeError::TracingBridge(e) => {
                write!(f, "Failed to initialize tracing bridge: {e}")
            }
            BridgeError::NoBridges => write!(
                f,
                "No bridges available (enable log-compat or tracing-compat features)"
            ),
        }
    }
}

impl std::error::Error for BridgeError {}

/// Initialize all available bridges
///
/// This will attempt to set up bridges for both `log` and `tracing` crates
/// if their respective features are enabled.
///
/// # Example
/// ```no_run
/// use proven_logger::{StdoutLogger, compat::init_bridges};
/// use std::sync::Arc;
///
/// let logger = Arc::new(StdoutLogger::new());
///
/// // Initialize proven-logger
/// proven_logger::init(logger.clone()).expect("Failed to init logger");
///
/// // Set up bridges for other logging crates
/// init_bridges(logger).expect("Failed to init bridges");
/// ```
pub fn init_bridges(logger: Arc<dyn Logger>) -> Result<(), BridgeError> {
    let mut initialized = false;

    // Try to initialize log bridge
    #[cfg(feature = "log-compat")]
    {
        if let Err(_e) = super::log_bridge::init_log_bridge(logger.clone()) {
            // Only fail if it's not already set
            // SetLoggerError means logger was already set, which is OK
            eprintln!("Note: log bridge already initialized");
        } else {
            initialized = true;
        }
    }

    // Try to initialize tracing bridge
    #[cfg(feature = "tracing-compat")]
    {
        if let Err(e) = super::tracing_bridge::init_tracing_bridge(logger) {
            // Check if it's because a subscriber is already set
            let error_str = e.to_string();
            if !error_str.contains("subscriber already exists") {
                return Err(BridgeError::TracingBridge(e));
            }
        } else {
            initialized = true;
        }
    }

    if initialized {
        Ok(())
    } else {
        Err(BridgeError::NoBridges)
    }
}

/// Initialize proven-logger with automatic bridging
///
/// This is a convenience function that:
/// 1. Initializes the global proven-logger
/// 2. Sets up bridges for `log` and `tracing` crates
///
/// # Example
/// ```no_run
/// use proven_logger::{StdoutLogger, compat::init_with_bridges};
/// use std::sync::Arc;
///
/// let logger = Arc::new(StdoutLogger::new());
/// init_with_bridges(logger).expect("Failed to initialize logging");
///
/// // Now all three work:
/// proven_logger::info!("From proven-logger");
/// log::info!("From log crate");
/// tracing::info!("From tracing crate");
/// ```
pub fn init_with_bridges(logger: Arc<dyn Logger>) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize our logger first
    crate::init(logger.clone()).map_err(|_| "Logger already initialized")?;

    // Then set up bridges (ignore errors if bridges already set)
    match init_bridges(logger) {
        Ok(()) => Ok(()),
        Err(BridgeError::NoBridges) => Ok(()), // No bridges is OK
        Err(e) => {
            // Log the error but don't fail
            eprintln!("Warning: Failed to initialize some bridges: {e}");
            Ok(())
        }
    }
}
