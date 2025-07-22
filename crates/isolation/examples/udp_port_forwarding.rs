//! Example showing how to access an isolated UDP echo server.
//!
//! This example demonstrates:
//! 1. Starting an isolated UDP echo server
//! 2. Working with an application that binds to a port
//! 3. Ensuring we can access the UDP server from the host

use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use proven_isolation::{IsolatedApplication, ReadyCheckInfo, VolumeMount};
use proven_logger::{StdoutLogger, debug, info, init};

/// The port that the UDP server will listen on
const SERVER_PORT: u16 = 8080;

/// Test message to send to the UDP server
const UDP_TEST_MESSAGE: &str = "Hello UDP Server!";

/// Buffer size for UDP responses
const UDP_BUFFER_SIZE: usize = 1024;

/// Application that runs a UDP echo server in an isolated namespace with port forwarding
struct UdpEchoServer {
    /// The path to the test bin directory
    test_bin_dir: PathBuf,
}

impl UdpEchoServer {
    /// Create a new server with port forwarding
    fn new(test_bin_dir: PathBuf) -> Self {
        Self { test_bin_dir }
    }
}

#[async_trait]
impl IsolatedApplication for UdpEchoServer {
    fn args(&self) -> Vec<String> {
        vec![]
    }

    fn executable(&self) -> &str {
        "/bin/udp_server"
    }

    async fn is_ready_check(&self, info: ReadyCheckInfo) -> bool {
        // Check if the UDP server is ready by sending a test packet
        static ATTEMPT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);
        let attempt = ATTEMPT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // First attempt will give the server time to start
        if attempt == 1 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            debug!("UDP server readiness check: initial 1 second wait");
            return false;
        }

        // Use the container's IP from the process
        let addr = SocketAddr::new(info.ip_address, SERVER_PORT);

        debug!(
            "Readiness check attempt {}: Connecting to {}",
            attempt, addr
        );

        // Create socket for testing server
        let test_result = || -> std::io::Result<bool> {
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
                        debug!("Received unexpected response: {received}");
                        Ok(false)
                    }
                }
                Err(e) => {
                    debug!("Failed to receive response: {e}");
                    Ok(false)
                }
            }
        };

        match test_result() {
            Ok(true) => {
                info!("✅ UDP server is ready! Got echo response on attempt {attempt}");
                true
            }
            Ok(false) => {
                debug!("❌ UDP server not ready on attempt {attempt}");
                false
            }
            Err(e) => {
                debug!("❌ Error testing UDP server on attempt {attempt}: {e}");
                false
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
        "udp-echo-server"
    }

    /// Return the UDP ports to forward
    fn udp_port_forwards(&self) -> Vec<u16> {
        vec![SERVER_PORT]
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        vec![VolumeMount::new(&self.test_bin_dir, &PathBuf::from("/bin"))]
    }
}

#[tokio::main]
async fn main() {
    // Initialize logger
    let logger = Arc::new(StdoutLogger::new());
    init(logger).expect("Failed to initialize logger");

    info!("🚀 Starting isolated UDP echo server example");
    info!(
        "This example demonstrates running a UDP server in isolation and accessing it from the host"
    );

    // Create dir for test binary
    let test_bin_dir = tempfile::tempdir().expect("Failed to create test bin directory");
    let test_bin_dir_path = test_bin_dir.keep();

    // Compile the UDP server statically and copy to test bin dir
    let server_c_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/udp_port_forwarding/server.c");
    let server_path = test_bin_dir_path.join("udp_server");

    debug!("Compiling UDP echo server from {}", server_c_path.display());

    let output = tokio::process::Command::new("gcc")
        .arg("-static")
        .arg("-o")
        .arg(&server_path)
        .arg(&server_c_path)
        .output()
        .await
        .expect("Failed to compile UDP server");

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("Failed to compile UDP server: {stderr}");
    }

    // Create the server
    let server = UdpEchoServer::new(test_bin_dir_path);

    // Spawn the isolated process
    info!("🔄 Spawning server process and waiting for it to become ready...");
    info!("🔌 The server will be accessible on localhost:{SERVER_PORT}");

    let start_time = Instant::now();
    let process = proven_isolation::spawn(server)
        .await
        .expect("Failed to spawn server");
    let elapsed = start_time.elapsed();
    info!("✅ Server process is now ready! (took {elapsed:?})");
    info!("Server is running with PID: {}", process.pid());

    // Make final requests to verify server connectivity
    info!("Making final requests to verify server and port forwarding");

    // Test sending to the container IP directly
    let container_ip = process.container_ip().unwrap();
    info!("Testing direct connection to container at {container_ip}:{SERVER_PORT}");

    let socket = UdpSocket::bind("0.0.0.0:0").expect("Failed to bind socket");
    socket
        .connect(SocketAddr::new(container_ip, SERVER_PORT))
        .expect("Failed to connect to container");

    // Send a test message
    socket
        .send(UDP_TEST_MESSAGE.as_bytes())
        .expect("Failed to send message to container");

    // Wait for response
    let mut buf = [0; UDP_BUFFER_SIZE];
    let len = socket
        .recv(&mut buf)
        .expect("Failed to receive from container");

    let received = String::from_utf8_lossy(&buf[..len]);
    info!("Direct container response: {received}");

    // Test localhost port forwarding
    info!("Testing localhost port forwarding at 127.0.0.1:{SERVER_PORT}");

    let socket = UdpSocket::bind("0.0.0.0:0").expect("Failed to bind socket");
    socket
        .connect(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            SERVER_PORT,
        ))
        .expect("Failed to connect to localhost");

    // Send a test message
    socket
        .send(UDP_TEST_MESSAGE.as_bytes())
        .expect("Failed to send message to localhost");

    // Wait for response
    let mut buf = [0; UDP_BUFFER_SIZE];
    let len = socket
        .recv(&mut buf)
        .expect("Failed to receive from localhost");

    let received = String::from_utf8_lossy(&buf[..len]);
    info!("Localhost port forwarding response: {received}");

    // Wait a bit before shutting down
    info!("Server verified working, waiting 5 seconds before shutdown");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Shut down the server
    info!("🛑 Shutting down server...");
    process.shutdown().await.expect("Failed to shutdown server");

    info!("✅ Example completed successfully");
}
