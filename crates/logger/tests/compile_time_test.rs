//! Test compile-time optimization

use proven_logger::*;
use std::sync::Arc;

// This test verifies that with max-level features, the compiler
// can optimize away logger instance method calls

#[cfg(feature = "max-level-off")]
#[test]
fn test_compile_time_optimization() {
    let logger = Arc::new(NoOpLogger);

    // With max-level-off, these should compile to nothing
    logger.error("This should be optimized away");
    logger.warn("This too");
    logger.info("And this");
    logger.debug("Also this");
    logger.trace("Finally this");

    // The compiler should be able to see that Level::Error.is_enabled_static()
    // is const false, so the entire if block gets eliminated
}

#[test]
fn test_logger_with_context() {
    #[cfg(feature = "stdout")]
    {
        let base_logger: Arc<dyn Logger> = Arc::new(StdoutLogger::new());
        let logger = base_logger.with_context(Context::new("test").with_node_id("node123"));

        // These use the LoggerExt trait methods
        logger.info("Message with context");
        logger.debug("Debug with context");
    }
}
