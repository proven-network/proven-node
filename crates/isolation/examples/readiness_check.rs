//! Example of process readiness checking using an isolated HTTP server.
//!
//! This example demonstrates:
//! 1. Starting an isolated process (HTTP server with delayed startup)
//! 2. Using readiness checks to determine when the server is actually ready
//! 3. Connecting to the server once it's ready
//!
//! The server will delay its startup to show the readiness check mechanism in action.

use std::error::Error as StdError;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use proven_isolation::{IsolatedApplication, IsolatedProcess};
use reqwest::StatusCode;
use tracing::info;

/// The port that the server will listen on
const SERVER_PORT: u16 = 8080;

/// Application that runs an HTTP server with delayed startup
struct ReadinessCheckServer {
    /// The path to the test bin directory
    test_bin_dir: PathBuf,

    /// Time to wait before server startup (in seconds)
    startup_delay: u32,
}

impl ReadinessCheckServer {
    /// Create a new readiness check server
    fn new(test_bin_dir: PathBuf, startup_delay: u32) -> Self {
        Self {
            test_bin_dir,
            startup_delay,
        }
    }
}

#[async_trait]
impl IsolatedApplication for ReadinessCheckServer {
    fn args(&self) -> Vec<String> {
        // Pass the startup delay as a command-line argument
        vec![self.startup_delay.to_string()]
    }

    fn executable(&self) -> &str {
        "/bin/http_server"
    }

    fn name(&self) -> &str {
        "readiness-check-server"
    }

    fn volume_mounts(&self) -> Vec<proven_isolation::VolumeMount> {
        vec![
            // Mount the test bin directory
            proven_isolation::VolumeMount::new(&self.test_bin_dir, &PathBuf::from("/bin")),
        ]
    }

    async fn is_ready_check(&self, _process: &IsolatedProcess) -> Result<bool, Box<dyn StdError>> {
        // Check if the HTTP server is ready by making a request
        static ATTEMPT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);
        let attempt = ATTEMPT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let url = format!("http://127.0.0.1:{}", SERVER_PORT);
        println!("Readiness check attempt {}: Connecting to {}", attempt, url);

        match reqwest::get(&url).await {
            Ok(response) => {
                if response.status() == StatusCode::OK {
                    println!(
                        "✅ Server is ready! Got HTTP 200 OK response on attempt {}",
                        attempt
                    );
                    Ok(true)
                } else {
                    println!(
                        "❌ Server responded with non-OK status: {} on attempt {}",
                        response.status(),
                        attempt
                    );
                    Ok(false)
                }
            }
            Err(e) => {
                println!(
                    "❌ Failed to connect to server on attempt {}: {}",
                    attempt, e
                );
                Ok(false)
            }
        }
    }

    fn is_ready_check_interval_ms(&self) -> u64 {
        1000 // Check every second
    }

    fn is_ready_check_max(&self) -> Option<u32> {
        Some(30) // Allow up to 30 seconds for readiness
    }
}

#[tokio::main]
async fn main() {
    // Initialize tracing with defaults
    tracing_subscriber::fmt().init();

    info!("🚀 Starting readiness check example");
    info!("This example demonstrates waiting for a process to become ready before continuing");
    info!(
        "The HTTP server will intentionally delay startup for 5 seconds to demonstrate the readiness check"
    );

    // Create dir for test binary
    let test_bin_dir = tempfile::tempdir().expect("Failed to create test bin directory");
    let test_bin_dir_path = test_bin_dir.into_path();

    // Compile the HTTP server statically and copy to test bin dir
    let server_c_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/readiness_check/server.c");
    let server_path = test_bin_dir_path.join("http_server");

    tokio::process::Command::new("gcc")
        .arg("-static")
        .arg("-o")
        .arg(&server_path)
        .arg(&server_c_path)
        .output()
        .await
        .expect("Failed to compile HTTP server");

    info!("The server is configured to delay startup for 3 seconds");

    // Create the server with a 3 second startup delay
    let server = ReadinessCheckServer::new(test_bin_dir_path, 3);

    // Spawn the isolated process
    info!("🔄 Spawning server process and waiting for it to become ready...");
    info!("Readiness will be checked once per second with up to 30 retries");

    let start_time = std::time::Instant::now();
    let (process, _join_handle) = proven_isolation::spawn(server)
        .await
        .expect("Failed to spawn server");
    let elapsed = start_time.elapsed();

    // The spawn method will already have waited for the server to be ready
    // using our is_ready_check implementation
    info!("✅ Server process is now ready! (took {:?})", elapsed);
    info!("Server is running with PID: {:?}", process.pid());

    // Make an HTTP request to the server to demonstrate it's working after the readiness check
    info!("📡 Making a request to the server...");
    let url = format!("http://127.0.0.1:{}", SERVER_PORT);
    let response = reqwest::get(&url)
        .await
        .expect("Failed to connect to server");

    info!("📨 Server response: {:?}", response.status());
    let body = response.text().await.expect("Failed to read response");
    info!("📄 Response body: \"{}\"", body);

    // Wait a bit before shutting down
    info!("⏳ Waiting 2 seconds before shutting down...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Shutdown the server by ending the process
    info!("🛑 Shutting down server...");
    process.shutdown().await.expect("Failed to shutdown server");

    info!("✅ Example completed successfully");
}
