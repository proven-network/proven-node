//! Example showing how to access an isolated UDP echo server.
//!
//! This example demonstrates:
//! 1. Starting an isolated UDP echo server
//! 2. Working with an application that binds to a port
//! 3. Ensuring we can access the UDP server from the host

use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use proven_isolation::{
    Error, IsolatedApplication, IsolatedProcess, IsolationConfig, IsolationManager,
    NamespaceOptions, Result,
};
use tokio::fs;
use tracing::{debug, info};

/// The port that the UDP server will listen on
const SERVER_PORT: u16 = 8080;

/// Test message to send to the UDP server
const UDP_TEST_MESSAGE: &str = "Hello UDP Server!";

/// Buffer size for UDP responses
const UDP_BUFFER_SIZE: usize = 1024;

/// Application that runs a UDP echo server in an isolated namespace with port forwarding
struct UdpEchoServer {
    /// The executable path
    executable_path: PathBuf,
}

impl UdpEchoServer {
    /// Create a new server with port forwarding
    async fn new(root_dir: &Path) -> Result<Self> {
        // Create a minimal filesystem inside the root directory
        let bin_dir = root_dir.join("bin");
        fs::create_dir_all(&bin_dir)
            .await
            .map_err(|e| Error::Io("Failed to create bin directory", e))?;

        // Get the path to the C source file
        let server_c_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/udp_port_forwarding/server.c");

        debug!("Compiling UDP echo server from {}", server_c_path.display());

        // Compile the UDP server
        let output = tokio::process::Command::new("gcc")
            .arg("-o")
            .arg(bin_dir.join("udp_server"))
            .arg(&server_c_path)
            .output()
            .await
            .map_err(|e| Error::Io("Failed to compile UDP server", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Io(
                "Failed to compile UDP server",
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
            let server_path = bin_dir.join("udp_server");
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
            executable_path: bin_dir.join("udp_server"),
        })
    }
}

#[async_trait]
impl IsolatedApplication for UdpEchoServer {
    fn args(&self) -> Vec<String> {
        vec![]
    }

    fn executable(&self) -> &str {
        self.executable_path.to_str().unwrap_or("/bin/udp_server")
    }

    fn name(&self) -> &str {
        "udp-echo-server"
    }

    fn namespace_options(&self) -> NamespaceOptions {
        let mut options = NamespaceOptions::default();
        options.use_network = true; // Always use network namespace for this example
        options
    }

    /// Return the UDP ports to forward
    fn udp_port_forwards(&self) -> Vec<u16> {
        vec![SERVER_PORT]
    }

    async fn is_ready_check(&self, process: &IsolatedProcess) -> Result<bool> {
        // Check if the UDP server is ready by sending a test packet
        static ATTEMPT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);
        let attempt = ATTEMPT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // First attempt will give the server time to start
        if attempt == 1 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            debug!("UDP server readiness check: initial 1 second wait");
            return Ok(false);
        }

        // Use the container's IP from the process
        let addr = if let Some(ip) = process.container_ip() {
            SocketAddr::new(ip, SERVER_PORT)
        } else {
            return Ok(false);
        };

        debug!(
            "Readiness check attempt {}: Connecting to {}",
            attempt, addr
        );

        // Create socket for testing server
        let test_result = || -> std::result::Result<bool, std::io::Error> {
            let socket = UdpSocket::bind("0.0.0.0:0")?;
            socket.connect(addr)?;
            socket.set_read_timeout(Some(Duration::from_secs(1)))?;

            // Send test message
            socket.send(UDP_TEST_MESSAGE.as_bytes())?;

            // Wait for response
            let mut buf = [0; UDP_BUFFER_SIZE];
            match socket.recv(&mut buf) {
                Ok(len) => {
                    let received = String::from_utf8_lossy(&buf[..len]);
                    if received == UDP_TEST_MESSAGE {
                        Ok(true) // Server is ready
                    } else {
                        debug!("Received unexpected response: {}", received);
                        Ok(false)
                    }
                }
                Err(e) => {
                    debug!("Failed to receive response: {}", e);
                    Ok(false)
                }
            }
        };

        match test_result() {
            Ok(true) => {
                info!(
                    "✅ UDP server is ready! Got echo response on attempt {}",
                    attempt
                );
                Ok(true)
            }
            Ok(false) => {
                debug!("❌ UDP server not ready on attempt {}", attempt);
                Ok(false)
            }
            Err(e) => {
                debug!("❌ Error testing UDP server on attempt {}: {}", attempt, e);
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

    info!("🚀 Starting isolated UDP echo server example");
    info!(
        "This example demonstrates running a UDP server in isolation and accessing it from the host"
    );

    // Set up temporary directory
    let temp_dir =
        tempfile::tempdir().map_err(|e| Error::Io("Failed to create temporary directory", e))?;

    let root_dir = temp_dir.path().to_path_buf();
    info!("📁 Server root directory: {}", root_dir.display());

    // Create the server
    let server = UdpEchoServer::new(&root_dir).await?;

    // Configure the isolation manager
    let config = IsolationConfig {
        use_chroot: false,
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

    // Test sending to the container IP directly
    let container_ip = process.container_ip().unwrap();
    info!(
        "Testing direct connection to container at {}:{}",
        container_ip, SERVER_PORT
    );

    let socket = UdpSocket::bind("0.0.0.0:0")
        .map_err(|e| Error::Application(format!("Failed to bind socket: {}", e)))?;
    socket
        .connect(SocketAddr::new(container_ip, SERVER_PORT))
        .map_err(|e| Error::Application(format!("Failed to connect to container: {}", e)))?;

    // Send a test message
    socket
        .send(UDP_TEST_MESSAGE.as_bytes())
        .map_err(|e| Error::Application(format!("Failed to send message to container: {}", e)))?;

    // Wait for response
    let mut buf = [0; UDP_BUFFER_SIZE];
    let len = socket
        .recv(&mut buf)
        .map_err(|e| Error::Application(format!("Failed to receive from container: {}", e)))?;

    let received = String::from_utf8_lossy(&buf[..len]);
    info!("Direct container response: {}", received);

    // Test localhost port forwarding
    info!(
        "Testing localhost port forwarding at 127.0.0.1:{}",
        SERVER_PORT
    );

    let socket = UdpSocket::bind("0.0.0.0:0")
        .map_err(|e| Error::Application(format!("Failed to bind socket: {}", e)))?;
    socket
        .connect(SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            SERVER_PORT,
        ))
        .map_err(|e| Error::Application(format!("Failed to connect to localhost: {}", e)))?;

    // Send a test message
    socket
        .send(UDP_TEST_MESSAGE.as_bytes())
        .map_err(|e| Error::Application(format!("Failed to send message to localhost: {}", e)))?;

    // Wait for response
    let mut buf = [0; UDP_BUFFER_SIZE];
    let len = socket
        .recv(&mut buf)
        .map_err(|e| Error::Application(format!("Failed to receive from localhost: {}", e)))?;

    let received = String::from_utf8_lossy(&buf[..len]);
    info!("Localhost port forwarding response: {}", received);

    // Wait a bit before shutting down
    info!("Server verified working, waiting 5 seconds before shutdown");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Shut down the server
    info!("🛑 Shutting down server...");
    process.shutdown().await?;

    Ok(())
}
