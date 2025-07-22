//! Demonstrates the test support functionality

#[cfg(feature = "test-support")]
mod tests {
    use proven_logger::test_support::*;
    use proven_logger::*;

    #[test]
    fn test_capture_logger() {
        // Create a capture logger
        let capture = CaptureLogger::new();
        let logger: std::sync::Arc<dyn Logger> = std::sync::Arc::new(capture.clone());

        // Log directly to the logger instance
        logger.info("This is captured");
        logger.error("This is also captured");

        // Check the captured logs
        assert!(capture.contains("This is captured"));
        assert!(capture.contains("ERROR"));

        let logs = capture.logs();
        assert!(logs.contains("This is captured"));
        assert!(logs.contains("This is also captured"));
    }

    // This uses our logged_test attribute
    #[proven_logger::logged_test]
    fn test_that_passes() {
        info!("Setting up test");
        debug!("Running assertions");
        assert_eq!(2 + 2, 4);
        info!("Test completed successfully");
        // These logs won't be printed because the test passes
    }

    #[proven_logger::logged_test]
    #[should_panic]
    fn test_that_fails() {
        info!("Starting failing test");
        warn!("About to fail");
        assert_eq!(2 + 2, 5); // This fails
        // When this test fails, all the logs will be printed
    }

    #[test]
    fn test_with_manual_guard() {
        let mut guard = TestLogGuard::new("manual_test");

        info!("Manual test starting");

        // Simulate some work
        for i in 0..3 {
            debug!("Iteration {i}");
        }

        // Test passes, so mark it
        guard.passed();
        // Logs won't be printed
    }

    #[test]
    #[should_panic]
    fn test_guard_on_panic() {
        let _guard = TestLogGuard::new("panic_test");

        error!("About to panic!");
        debug!("This should be visible when test fails");

        panic!("Test panic");
        // The guard will print all logs when the test panics
    }
}
