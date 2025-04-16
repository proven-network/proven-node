//! Example showing how to access an isolated HTTP server.
//!
//! This example demonstrates:
//! 1. Starting an isolated HTTP server
//! 2. Working with an application that binds to a port
//! 3. Ensuring we can access the HTTP server from the host

use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use proven_isolation::{IsolatedApplication, IsolatedProcess, VolumeMount};
use reqwest::StatusCode;
use tracing::{debug, info};

/// The port that the server will listen on
const SERVER_PORT: u16 = 8080;

/// Application that runs an HTTP server in an isolated namespace with port forwarding
struct PortForwardServer {
    /// The path to the test bin directory
    test_bin_dir: PathBuf,
}

impl PortForwardServer {
    /// Create a new server with port forwarding
    fn new(test_bin_dir: PathBuf) -> Self {
        Self { test_bin_dir }
    }
}

#[async_trait]
impl IsolatedApplication for PortForwardServer {
    fn args(&self) -> Vec<String> {
        vec![]
    }

    fn executable(&self) -> &str {
        "/bin/http_server"
    }

    async fn is_ready_check(
        &self,
        process: &IsolatedProcess,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        // Check if the HTTP server is ready by making a request
        static ATTEMPT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);
        let attempt = ATTEMPT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Use the container's IP from the process
        let url = if let Some(ip) = process.container_ip() {
            format!("http://{}:{}", ip, SERVER_PORT)
        } else {
            format!("http://127.0.0.1:{}", SERVER_PORT)
        };
        debug!("Readiness check attempt {}: Connecting to {}", attempt, url);

        match reqwest::get(&url).await {
            Ok(response) => {
                if response.status() == StatusCode::OK {
                    info!(
                        "✅ Server is ready! Got HTTP 200 OK response on attempt {}",
                        attempt
                    );
                    Ok(true)
                } else {
                    debug!(
                        "❌ Server responded with non-OK status: {} on attempt {}",
                        response.status(),
                        attempt
                    );
                    Ok(false)
                }
            }
            Err(e) => {
                debug!(
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
        Some(20) // Check up to 20 times
    }

    fn name(&self) -> &str {
        "port-forward-server"
    }

    fn tcp_port_forwards(&self) -> Vec<u16> {
        vec![SERVER_PORT]
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        vec![VolumeMount::new(&self.test_bin_dir, &PathBuf::from("/bin"))]
    }
}

#[tokio::main]
async fn main() {
    // Initialize tracing with defaults
    tracing_subscriber::fmt::init();

    info!("🚀 Starting isolated HTTP server example");
    info!("This example demonstrates running a server in isolation and accessing it from the host");

    // Create dir for test binary
    let test_bin_dir = tempfile::tempdir().expect("Failed to create test bin directory");
    let test_bin_dir_path = test_bin_dir.into_path();

    // Compile the HTTP server statically and copy to test bin dir
    let server_c_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/tcp_port_forwarding/server.c");
    let server_path = test_bin_dir_path.join("http_server");

    debug!("Compiling HTTP server from {}", server_c_path.display());

    let output = tokio::process::Command::new("gcc")
        .arg("-static")
        .arg("-o")
        .arg(&server_path)
        .arg(&server_c_path)
        .output()
        .await
        .expect("Failed to compile HTTP server");

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("Failed to compile HTTP server: {}", stderr);
    }

    // Create the server
    let server = PortForwardServer::new(test_bin_dir_path);

    // Spawn the isolated process
    info!("🔄 Spawning server process and waiting for it to become ready...");
    info!(
        "🔌 The server will be accessible on localhost:{}",
        SERVER_PORT
    );

    let start_time = std::time::Instant::now();
    let (process, _join_handle) = proven_isolation::spawn(server)
        .await
        .expect("Failed to spawn server");
    let elapsed = start_time.elapsed();
    info!("✅ Server process is now ready! (took {:?})", elapsed);
    info!("Server is running with PID: {}", process.pid());

    // Make final requests to verify server connectivity
    info!("Making final requests to verify server and port forwarding");

    // First verify direct container access via veth pair
    let response = reqwest::get(&format!(
        "http://{}:{}",
        process.container_ip().unwrap(),
        SERVER_PORT
    ))
    .await
    .expect("Failed to make direct container request");

    let text = response
        .text()
        .await
        .expect("Failed to read direct container response");
    info!("Direct container access response: {}", text);

    // Then verify localhost port forwarding
    let response = reqwest::get(&format!("http://127.0.0.1:{}", SERVER_PORT))
        .await
        .expect("Failed to make localhost request");

    let text = response
        .text()
        .await
        .expect("Failed to read localhost response");
    info!("Localhost port forwarding response: {}", text);

    // Wait a bit before shutting down
    info!("Server verified working, waiting 5 seconds before shutdown");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Shut down the server
    info!("🛑 Shutting down server...");
    process.shutdown().await.expect("Failed to shutdown server");

    info!("✅ Example completed successfully");
}
