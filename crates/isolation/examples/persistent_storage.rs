//! Example demonstrating persistent storage between runs of an IsolatedApplication.
//!
//! This example shows how to:
//! 1. Set up a persistent storage directory that survives between runs
//! 2. Mount this directory into the isolated environment using volume mounts
//! 3. Read and write files that persist between runs
//!
//! The example creates a simple counter application that:
//! 1. Reads a counter value from a file
//! 2. Increments it
//! 3. Writes it back
//! 4. Shows how the value persists between runs

use async_trait::async_trait;
use std::path::PathBuf;
use tokio::fs;
use tokio::process::Command;
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info, warn};

use proven_isolation::{IsolatedApplication, IsolationManager, VolumeMount};

/// A simple counter application that maintains state between runs
struct CounterApp {
    /// The path to the persistent storage directory
    storage_dir: PathBuf,

    /// The path to the test bin directory
    test_bin_dir: PathBuf,
}

impl CounterApp {
    /// Creates a new counter application
    fn new(storage_dir: PathBuf, test_bin_dir: PathBuf) -> Self {
        Self {
            storage_dir,
            test_bin_dir,
        }
    }

    /// Parse a log line and forward to the appropriate tracing macro
    fn parse_log_line(&self, line: &str) {
        // Parse the log line based on its prefix
        if line.starts_with("[INFO]") {
            let content = line.trim_start_matches("[INFO]").trim();
            info!(target: "counter-app", "{}", content);
        } else if line.starts_with("[DEBUG]") {
            let content = line.trim_start_matches("[DEBUG]").trim();
            debug!(target: "counter-app", "{}", content);
        } else if line.starts_with("[WARN]") {
            let content = line.trim_start_matches("[WARN]").trim();
            warn!(target: "counter-app", "{}", content);
        } else if line.starts_with("[ERROR]") {
            let content = line.trim_start_matches("[ERROR]").trim();
            error!(target: "counter-app", "{}", content);
        } else {
            // For unrecognized log formats, just log as info
            info!(target: "counter-app", "{}", line);
        }
    }
}

#[async_trait]
impl IsolatedApplication for CounterApp {
    fn args(&self) -> Vec<String> {
        Vec::new()
    }

    fn executable(&self) -> &str {
        "/bin/counter"
    }

    fn name(&self) -> &str {
        "counter-app"
    }

    fn handle_stdout(&self, line: &str) {
        self.parse_log_line(line);
    }

    fn handle_stderr(&self, line: &str) {
        error!(target: "counter-app", "{}", line);
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        let data_path = PathBuf::from("/data");
        vec![
            // Mount the persistent data directory
            VolumeMount::new(&self.storage_dir, &data_path),
            // Mount the test bin directory
            VolumeMount::new(&self.test_bin_dir, &PathBuf::from("/bin")),
        ]
    }
}

#[tokio::main]
async fn main() {
    // Set up logging
    tracing_subscriber::fmt::init();

    // Create dir for test binary
    let test_bin_dir = tempfile::tempdir().expect("Failed to create test bin directory");

    // Create a persistent storage directory
    let storage_dir = tempfile::tempdir().expect("Failed to create storage directory");

    // Create the isolation manager with our config
    let manager = IsolationManager::new();

    let storage_dir_path = storage_dir.into_path();
    let test_bin_dir_path = test_bin_dir.into_path();

    // Compile the counter program statically and copy to test bin dir
    let counter_c_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/persistent_storage/counter.c");
    let counter_path = test_bin_dir_path.join("counter");

    Command::new("gcc")
        .arg("-static")
        .arg("-o")
        .arg(&counter_path)
        .arg(&counter_c_path)
        .output()
        .await
        .expect("Failed to compile counter program");

    // Run the counter application multiple times to demonstrate persistence
    for run in 1..=3 {
        info!("Run #{}", run);

        let app = CounterApp::new(storage_dir_path.clone(), test_bin_dir_path.clone());
        let (process, _join_handle) = manager
            .spawn(app)
            .await
            .expect("Failed to spawn counter app");

        // Wait for the process to exit
        let exit_status = process.wait().await;
        println!("exit status: {:?}", exit_status);

        // Clean up the process
        process
            .shutdown()
            .await
            .expect("Failed to shutdown counter app");

        // Read the current counter value
        let counter_file = storage_dir_path.join("counter.txt");
        if counter_file.exists() {
            let value = fs::read_to_string(&counter_file)
                .await
                .expect("Failed to read counter file");
            info!("Counter value after run {}: {}", run, value.trim());
        }

        // Wait a bit between runs
        sleep(Duration::from_secs(1)).await;
    }
}
