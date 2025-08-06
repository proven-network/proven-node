//! Common test setup for integration tests

use proven_local_cluster::ClusterLogSystem;
use std::sync::Once;

static INIT: Once = Once::new();

/// Initialize the global tracing subscriber for all tests
/// This should be called at the beginning of each test module
pub fn init_test_logging() {
    INIT.call_once(|| {
        // Create a shared log system for all tests
        let log_dir = std::env::temp_dir().join("proven-cluster-tests");
        std::fs::create_dir_all(&log_dir).ok();

        // Use stdout logging for debugging
        let log_system =
            ClusterLogSystem::with_stdout(&log_dir).expect("Failed to create log system");

        // Set up the global subscriber - this can only be done once
        log_system
            .setup_global_subscriber()
            .expect("Failed to setup global subscriber");

        eprintln!("Global test logging initialized with stdout output");
    });
}
