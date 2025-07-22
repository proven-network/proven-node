//! Basic tests for the logger

use proven_logger::*;
use std::sync::Arc;

#[test]
#[cfg(feature = "stdout")]
fn test_macros() {
    // Initialize with stdout logger
    let logger = StdoutLogger::new().with_level(Level::Debug);
    init(Arc::new(logger)).ok();

    // Test macros
    error!("This is an error");
    warn!("This is a warning");
    info!("This is info");
    debug!("This is debug");
    trace!("This is trace (won't show with Debug level)");

    // Test formatting
    let value = 42;
    info!("The answer is {value}");
}

#[test]
#[cfg(feature = "stdout")]
fn test_context() {
    // Create a logger with context
    let base_logger = StdoutLogger::new();
    let logger = base_logger.with_context(Context::new("engine").with_node_id("abc123"));

    // Use it directly
    logger.info("Starting engine");
    logger.debug("Engine initialized");
}

#[test]
#[cfg(feature = "stdout")]
fn test_performance() {
    // Don't init to avoid conflicts with other tests
    let logger = Arc::new(StdoutLogger::new().with_level(Level::Info));

    // Test that debug level check works
    assert!(!logger.is_enabled(Level::Debug));
    assert!(logger.is_enabled(Level::Info));
    assert!(logger.is_enabled(Level::Error));
}

#[test]
fn test_noop_logger() {
    let logger = Arc::new(NoOpLogger);

    // NoOpLogger should never be enabled
    assert!(!logger.is_enabled(Level::Error));
    assert!(!logger.is_enabled(Level::Warn));
    assert!(!logger.is_enabled(Level::Info));
    assert!(!logger.is_enabled(Level::Debug));
    assert!(!logger.is_enabled(Level::Trace));

    // Test that it doesn't panic when used
    logger.error("This goes nowhere");
    logger.flush();
}
