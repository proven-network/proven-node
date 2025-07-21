//! Tests for compatibility bridges

#[cfg(any(feature = "log-compat", feature = "tracing-compat"))]
mod tests {
    use proven_logger::*;
    use std::sync::Arc;

    #[test]
    #[cfg(all(feature = "test-support", feature = "log-compat"))]
    fn test_log_bridge() {
        // Create a capture logger
        let capture = test_support::CaptureLogger::new();
        let logger: Arc<dyn Logger> = Arc::new(capture.clone());

        // Initialize the log bridge
        compat::log_bridge::init_log_bridge(logger).ok();

        // Use log macros
        log::error!("Error from log crate");
        log::warn!("Warning from log crate");
        log::info!("Info from log crate");
        log::debug!("Debug from log crate");

        // Check captures
        let logs = capture.logs();
        assert!(logs.contains("Error from log crate"));
        assert!(logs.contains("Warning from log crate"));
        assert!(logs.contains("Info from log crate"));
        assert!(logs.contains("Debug from log crate"));
    }

    #[test]
    #[cfg(all(feature = "test-support", feature = "tracing-compat"))]
    fn test_tracing_bridge() {
        // Create a capture logger
        let capture = test_support::CaptureLogger::new();
        let logger: Arc<dyn Logger> = Arc::new(capture.clone());

        // Initialize the tracing bridge
        compat::tracing_bridge::init_tracing_bridge(logger).ok();

        // Use tracing macros
        tracing::error!("Error from tracing");
        tracing::warn!("Warning from tracing");
        tracing::info!("Info from tracing");
        tracing::debug!("Debug from tracing");

        // Use tracing with fields
        tracing::info!(count = 42, "Message with field");

        // Check captures
        let logs = capture.logs();
        assert!(logs.contains("Error from tracing"));
        assert!(logs.contains("Warning from tracing"));
        assert!(logs.contains("Info from tracing"));
        assert!(logs.contains("Debug from tracing"));
        assert!(logs.contains("Message with field"));
        assert!(logs.contains("count=42"));
    }

    #[test]
    #[cfg(all(feature = "test-support", feature = "tracing-compat"))]
    fn test_tracing_spans() {
        // Create a capture logger
        let capture = test_support::CaptureLogger::new();
        let logger: Arc<dyn Logger> = Arc::new(capture.clone());

        // Initialize the tracing bridge
        compat::tracing_bridge::init_tracing_bridge(logger).ok();

        // Use tracing with spans
        let span = tracing::info_span!("my_span");
        let _enter = span.enter();

        tracing::info!("Inside span");

        // Check that span context is included
        let logs = capture.logs();
        assert!(logs.contains("my_span"));
        assert!(logs.contains("Inside span"));
    }

    #[test]
    #[cfg(all(feature = "log-compat", feature = "tracing-compat"))]
    fn test_both_bridges() {
        // Use the convenience function
        let logger = Arc::new(StdoutLogger::new());

        // This should set up both bridges
        compat::init_with_bridges(logger.clone()).expect("Failed to init with bridges");

        // Now both should work
        proven_logger::info!("From proven-logger");
        log::info!("From log crate");
        tracing::info!("From tracing crate");

        // No panics = success
    }
}
