//! Example of process readiness checking using an isolated HTTP server.
//!
//! This example demonstrates:
//! 1. Starting an isolated process (HTTP server with delayed startup)
//! 2. Using readiness checks to determine when the server is actually ready
//! 3. Connecting to the server once it's ready
//!
//! The server will delay its startup to show the readiness check mechanism in action.

use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use proven_isolation::{
    Error, IsolatedApplication, IsolatedProcess, IsolationConfig, IsolationManager,
    NamespaceOptions, Result,
};
use reqwest::StatusCode;
use tokio::fs;
use tracing::info;

/// The port that the server will listen on
const SERVER_PORT: u16 = 8080;

/// Application that runs an HTTP server with delayed startup
struct ReadinessCheckServer {
    /// The executable path
    executable_path: PathBuf,

    /// Time to wait before server startup (in seconds)
    startup_delay: u32,
}

impl ReadinessCheckServer {
    /// Create a new readiness check server
    async fn new(root_dir: &Path, startup_delay: u32) -> Result<Self> {
        // Create a minimal filesystem inside the root directory
        let bin_dir = root_dir.join("bin");
        fs::create_dir_all(&bin_dir)
            .await
            .map_err(|e| Error::Io("Failed to create bin directory", e))?;

        // Get the path to the C source file
        let server_c_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/readiness_check/server.c");

        // Compile the HTTP server
        let output = tokio::process::Command::new("gcc")
            .arg("-o")
            .arg(bin_dir.join("http_server"))
            .arg(&server_c_path)
            .output()
            .await
            .map_err(|e| Error::Io("Failed to compile HTTP server", e))?;

        if !output.status.success() {
            return Err(Error::Io(
                "Failed to compile HTTP server",
                std::io::Error::new(std::io::ErrorKind::Other, "Compilation failed"),
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

        // Debug the file system structure
        println!("Contents of root directory:");
        let mut entries = fs::read_dir(root_dir)
            .await
            .map_err(|e| Error::Io("Failed to read root directory", e))?;

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| Error::Io("Failed to read directory entry", e))?
        {
            println!("- {}", entry.path().display());
        }

        println!("Contents of bin directory:");
        let mut bin_entries = fs::read_dir(&bin_dir)
            .await
            .map_err(|e| Error::Io("Failed to read bin directory", e))?;

        while let Some(entry) = bin_entries
            .next_entry()
            .await
            .map_err(|e| Error::Io("Failed to read directory entry", e))?
        {
            println!("- {}", entry.path().display());
        }

        Ok(Self {
            executable_path: bin_dir.join("http_server"),
            startup_delay,
        })
    }
}

#[async_trait]
impl IsolatedApplication for ReadinessCheckServer {
    fn args(&self) -> Vec<String> {
        // Pass the startup delay as a command-line argument
        vec![self.startup_delay.to_string()]
    }

    fn executable(&self) -> &str {
        self.executable_path.to_str().unwrap_or("/bin/http_server")
    }

    fn name(&self) -> &str {
        "readiness-check-server"
    }

    fn namespace_options(&self) -> NamespaceOptions {
        // Use no isolation for this example
        NamespaceOptions::none()
    }

    async fn is_ready_check(&self, _process: &IsolatedProcess) -> Result<bool> {
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
async fn main() -> Result<()> {
    // Initialize tracing with defaults
    tracing_subscriber::fmt().init();

    info!("🚀 Starting readiness check example");
    info!("This example demonstrates waiting for a process to become ready before continuing");
    info!(
        "The HTTP server will intentionally delay startup for 5 seconds to demonstrate the readiness check"
    );

    // Set up temporary directory
    let temp_dir =
        tempfile::tempdir().map_err(|e| Error::Io("Failed to create temporary directory", e))?;

    let root_dir = temp_dir.path().to_path_buf();
    info!("📁 Server root directory: {}", root_dir.display());

    // Create the server with a 3 second startup delay (reduced for faster testing)
    let server = ReadinessCheckServer::new(&root_dir, 3).await?;

    // Configure the isolation manager with no isolation
    let config = IsolationConfig {
        use_chroot: false,
        use_ipc_namespace: false,
        use_memory_limits: false,
        use_mount_namespace: false,
        use_network_namespace: false,
        use_pid_namespace: false,
        use_user_namespace: false,
        use_uts_namespace: false,
    };

    info!("⚙️ Running with isolation config: {:?}", config);
    info!("The server is configured to delay startup for 3 seconds");

    let manager = IsolationManager::with_config(config);

    // Spawn the isolated process
    info!("🔄 Spawning server process and waiting for it to become ready...");
    info!("Readiness will be checked once per second with up to 30 retries");

    let start_time = std::time::Instant::now();
    let (process, _join_handle) = manager.spawn(server).await?;
    let elapsed = start_time.elapsed();

    // The spawn method will already have waited for the server to be ready
    // using our is_ready_check implementation
    info!("✅ Server process is now ready! (took {:?})", elapsed);
    info!("Server is running with PID: {:?}", process.pid());

    // Make an HTTP request to the server to demonstrate it's working
    info!("📡 Making a request to the server...");
    let url = format!("http://127.0.0.1:{}", SERVER_PORT);
    let response = reqwest::get(&url)
        .await
        .map_err(|e| Error::Application(format!("Failed to connect to server: {}", e)))?;

    info!("📨 Server response: {:?}", response.status());
    let body = response
        .text()
        .await
        .map_err(|e| Error::Application(format!("Failed to read response: {}", e)))?;
    info!("📄 Response body: \"{}\"", body);

    // Wait a bit before shutting down
    info!("⏳ Waiting 2 seconds before shutting down...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Shutdown the server by ending the process
    info!("🛑 Shutting down server...");
    process.shutdown().await?;

    // Clean up temporary directory
    drop(temp_dir);

    info!("✅ Example completed successfully");
    Ok(())
}
