//! Example showing how to access an isolated HTTP server.
//!
//! This example demonstrates:
//! 1. Starting an isolated HTTP server
//! 2. Working with an application that binds to a port
//! 3. Ensuring we can access the HTTP server from the host

use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use proven_isolation::{
    Error, IsolatedApplication, IsolatedProcess, IsolationConfig, IsolationManager,
    NamespaceOptions, Result,
};
use reqwest::StatusCode;
use tokio::fs;
use tracing::{debug, info};

/// The port that the server will listen on
const SERVER_PORT: u16 = 8080;

/// Application that runs an HTTP server in an isolated namespace with port forwarding
struct PortForwardServer {
    /// The executable path
    executable_path: PathBuf,
}

impl PortForwardServer {
    /// Create a new server with port forwarding
    async fn new(root_dir: &Path) -> Result<Self> {
        // Create a minimal filesystem inside the root directory
        let bin_dir = root_dir.join("bin");
        fs::create_dir_all(&bin_dir)
            .await
            .map_err(|e| Error::Io("Failed to create bin directory", e))?;

        // Get the path to the C source file
        let server_c_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/tcp_port_forwarding/server.c");

        debug!("Compiling HTTP server from {}", server_c_path.display());

        // Compile the HTTP server
        let output = tokio::process::Command::new("gcc")
            .arg("-o")
            .arg(bin_dir.join("http_server"))
            .arg(&server_c_path)
            .output()
            .await
            .map_err(|e| Error::Io("Failed to compile HTTP server", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Io(
                "Failed to compile HTTP server",
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Compilation failed: {}", stderr),
                ),
            ));
        }

        // Make the program executable
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let server_path = bin_dir.join("http_server");
            let mut perms = tokio::fs::metadata(&server_path)
                .await
                .map_err(|e| Error::Io("Failed to get server metadata", e))?
                .permissions();

            perms.set_mode(0o755);
            tokio::fs::set_permissions(&server_path, perms)
                .await
                .map_err(|e| Error::Io("Failed to set server permissions", e))?;
        }

        Ok(Self {
            executable_path: bin_dir.join("http_server"),
        })
    }
}

#[async_trait]
impl IsolatedApplication for PortForwardServer {
    fn args(&self) -> Vec<String> {
        vec![]
    }

    fn executable(&self) -> &str {
        self.executable_path.to_str().unwrap_or("/bin/http_server")
    }

    fn name(&self) -> &str {
        "port-forward-server"
    }

    fn namespace_options(&self) -> NamespaceOptions {
        let mut options = NamespaceOptions::default();
        options.use_network = true; // Always use network namespace for this example
        options
    }

    /// Return the TCP ports to forward
    fn tcp_port_forwards(&self) -> Vec<u16> {
        vec![SERVER_PORT]
    }

    async fn is_ready_check(&self, process: &IsolatedProcess) -> Result<bool> {
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
        Some(20) // Allow up to 20 seconds for readiness
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing with defaults
    tracing_subscriber::fmt::init();

    info!("🚀 Starting isolated HTTP server example");
    info!("This example demonstrates running a server in isolation and accessing it from the host");

    // Set up temporary directory
    let temp_dir =
        tempfile::tempdir().map_err(|e| Error::Io("Failed to create temporary directory", e))?;

    let root_dir = temp_dir.path().to_path_buf();
    info!("📁 Server root directory: {}", root_dir.display());

    // Create the server
    let server = PortForwardServer::new(&root_dir).await?;

    // Configure the isolation manager
    let config = IsolationConfig {
        use_ipc_namespace: true,
        use_memory_limits: false,
        use_mount_namespace: true,
        use_network_namespace: true,
        use_pid_namespace: false,
        use_user_namespace: true,
        use_uts_namespace: true,
    };

    info!("⚙️ Running with isolation config: {:?}", config);

    let manager = IsolationManager::with_config(config);

    // Spawn the isolated process
    info!("🔄 Spawning server process and waiting for it to become ready...");
    info!(
        "🔌 The server will be accessible on localhost:{}",
        SERVER_PORT
    );

    let start_time = std::time::Instant::now();
    let (process, _join_handle) = manager.spawn(server).await?;
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
    .map_err(|e| Error::Application(format!("Failed to make direct container request: {}", e)))?;
    let text = response.text().await.map_err(|e| {
        Error::Application(format!("Failed to read direct container response: {}", e))
    })?;
    info!("Direct container access response: {}", text);

    // Then verify localhost port forwarding
    let response = reqwest::get(&format!("http://127.0.0.1:{}", SERVER_PORT))
        .await
        .map_err(|e| Error::Application(format!("Failed to make localhost request: {}", e)))?;
    let text = response
        .text()
        .await
        .map_err(|e| Error::Application(format!("Failed to read localhost response: {}", e)))?;
    info!("Localhost port forwarding response: {}", text);

    // Wait a bit before shutting down
    info!("Server verified working, waiting 5 seconds before shutdown");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Shut down the server
    info!("🛑 Shutting down server...");
    process.shutdown().await?;

    Ok(())
}
