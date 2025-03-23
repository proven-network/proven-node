//! Configures and runs the Reth Ethereum execution client.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::{Error, Result};

use std::fs;
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::process::Stdio;
use std::str;

use nix::sys::signal;
use nix::sys::signal::Signal;
use nix::unistd::Pid;
use reqwest::Client;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

/// Represents an Ethereum network
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EthereumNetwork {
    /// Ethereum mainnet
    Mainnet,
    /// Ethereum testnet (Sepolia)
    Sepolia,
    /// Ethereum testnet (Holesky)
    Holesky,
}

impl EthereumNetwork {
    /// Returns the network name as a string
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Sepolia => "sepolia",
            Self::Holesky => "holesky",
        }
    }
}

/// Runs a Reth execution client.
pub struct RethNode {
    discovery_addr: SocketAddrV4,
    http_addr: SocketAddrV4,
    metrics_addr: SocketAddrV4,
    network: EthereumNetwork,
    rpc_addr: SocketAddrV4,
    shutdown_token: CancellationToken,
    store_dir: PathBuf,
    task_tracker: TaskTracker,
}

/// Options for configuring a `RethNode`.
pub struct RethNodeOptions {
    /// The peer discovery socket address.
    pub discovery_addr: SocketAddrV4,

    /// The HTTP RPC socket address.
    pub http_addr: SocketAddrV4,

    /// The metrics socket address.
    pub metrics_addr: SocketAddrV4,

    /// The Ethereum network to connect to.
    pub network: EthereumNetwork,

    /// The RPC socket address.
    pub rpc_addr: SocketAddrV4,

    /// The directory to store data in.
    pub store_dir: PathBuf,
}

impl RethNode {
    /// Create a new Reth node.
    #[must_use]
    pub fn new(
        RethNodeOptions {
            discovery_addr,
            http_addr,
            metrics_addr,
            network,
            rpc_addr,
            store_dir,
        }: RethNodeOptions,
    ) -> Self {
        Self {
            discovery_addr,
            http_addr,
            metrics_addr,
            network,
            rpc_addr,
            shutdown_token: CancellationToken::new(),
            store_dir,
            task_tracker: TaskTracker::new(),
        }
    }

    /// Start the Reth node.
    ///
    /// Returns a handle to the task that is running the node.
    pub async fn start(&self) -> Result<JoinHandle<()>> {
        // Ensure data directories exist
        self.create_data_directories()?;

        // Start Reth and handle it in a single task
        let discovery_addr = self.discovery_addr;
        let http_addr = self.http_addr;
        let metrics_addr = self.metrics_addr;
        let network = self.network;
        let rpc_addr = self.rpc_addr;
        let shutdown_token = self.shutdown_token.clone();
        let store_dir = self.store_dir.clone();
        let task_tracker = self.task_tracker.clone();

        let server_task = self.task_tracker.spawn(async move {
            let mut cmd = Command::new("reth");
            cmd.args(["node", "--full", "--datadir", store_dir.to_str().unwrap()]);

            // Add network-specific args
            match network {
                EthereumNetwork::Mainnet => {
                    // Mainnet is default, no args needed
                }
                EthereumNetwork::Sepolia => {
                    cmd.arg("--chain=sepolia");
                }
                EthereumNetwork::Holesky => {
                    cmd.arg("--chain=holesky");
                }
            }

            // Configure Auth server
            cmd.args([
                "--authrpc.addr",
                &rpc_addr.ip().to_string(),
                "--authrpc.port",
                &rpc_addr.port().to_string(),
            ]);

            // Enable HTTP-RPC server with configured address
            cmd.args([
                "--http", // Enable HTTP-RPC server
                "--http.addr",
                &http_addr.ip().to_string(), // Use configured IP
                "--http.port",
                &http_addr.port().to_string(), // Use configured port
                "--http.api",
                "eth,net,web3,txpool", // Enable common APIs
            ]);

            // Configure peer discovery
            cmd.args([
                "--discovery.addr",
                &discovery_addr.ip().to_string(),
                "--discovery.port",
                &discovery_addr.port().to_string(),
            ]);

            // Configure metrics
            cmd.args([
                "--metrics",
                format!("{}:{}", metrics_addr.ip(), metrics_addr.port()).as_str(),
            ]);

            info!("Starting Reth with command: {:?}", cmd);

            cmd.stdout(Stdio::piped());
            cmd.stderr(Stdio::piped());

            let mut child = match cmd.spawn() {
                Ok(child) => child,
                Err(e) => {
                    error!("Failed to spawn reth: {}", e);
                    return;
                }
            };

            let Some(stdout) = child.stdout.take() else {
                error!("Failed to capture stdout");
                return;
            };

            let Some(stderr) = child.stderr.take() else {
                error!("Failed to capture stderr");
                return;
            };

            let stdout_reader = BufReader::new(stdout).lines();
            let stderr_reader = BufReader::new(stderr).lines();

            // Parse and log output
            let geth_stdout_task = task_tracker.spawn(async move {
                let mut lines = stdout_reader;
                while let Ok(Some(line)) = lines.next_line().await {
                    // Removing ANSI escape codes for cleaner logs
                    let bytes = strip_ansi_escapes::strip(&line);
                    // Skip logging if we don't have valid UTF-8
                    if let Ok(s) = std::str::from_utf8(&bytes) {
                        info!(target: "reth", "{s}");
                    }
                }
            });

            let geth_stderr_task = task_tracker.spawn(async move {
                let mut lines = stderr_reader;
                while let Ok(Some(line)) = lines.next_line().await {
                    // Removing ANSI escape codes for cleaner logs
                    let bytes = strip_ansi_escapes::strip(&line);
                    // Skip logging if we don't have valid UTF-8
                    if let Ok(s) = std::str::from_utf8(&bytes) {
                        info!(target: "reth", "{s}");
                    }
                }
            });

            // Wait for the process to exit or for the shutdown token to be cancelled
            tokio::select! {
                status_result = child.wait() => {
                    // Process exited on its own
                    let status = match status_result {
                        Ok(status) => status,
                        Err(e) => {
                            error!("Failed to wait for reth: {}", e);
                            return;
                        }
                    };

                    // Clean up the stdout/stderr tasks
                    geth_stdout_task.abort();
                    geth_stderr_task.abort();

                    if !status.success() {
                        error!("Reth exited with non-zero status: {}", status);
                        return;
                    }

                    info!("Reth process completed successfully");
                }
                () = shutdown_token.cancelled() => {
                    info!("Shutdown requested, terminating Reth process");

                    // Get the PID and send SIGTERM
                    if let Some(id) = child.id() {
                        let Ok(raw_pid) = id.try_into() else {
                            error!("Failed to convert process ID");
                            return;
                        };

                        let pid = Pid::from_raw(raw_pid);
                        if let Err(e) = signal::kill(pid, Signal::SIGINT) {
                            error!("Failed to send SIGINT to Reth process: {}", e);
                        }
                    } else {
                        error!("Could not get Reth process ID for termination");
                    }

                    // Wait for the process to exit
                    let _ = child.wait().await;

                    // Clean up the stdout/stderr tasks
                    geth_stdout_task.abort();
                    geth_stderr_task.abort();

                    info!("Reth process terminated");
                }
            }
        });

        self.task_tracker.close();

        // Wait for the node to be ready
        self.wait_until_ready().await?;

        Ok(server_task)
    }

    /// Shutdown the Reth node.
    pub async fn shutdown(&self) {
        info!("Reth node shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("Reth node shutdown");
    }

    fn create_data_directories(&self) -> Result<()> {
        if !self.store_dir.exists() {
            fs::create_dir_all(&self.store_dir)
                .map_err(|e| Error::Io("failed to create reth directory", e))?;
        }

        Ok(())
    }

    async fn wait_until_ready(&self) -> Result<()> {
        // Simple health check - wait for RPC to be ready
        let client = Client::new();
        let url = format!("http://{}:{}", self.rpc_addr.ip(), self.rpc_addr.port());

        // Try to connect for up to 60 seconds
        for _ in 0..60 {
            if client
                .post(&url)
                .json(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": "web3_clientVersion",
                    "params": [],
                    "id": 1
                }))
                .send()
                .await
                .is_ok()
            {
                info!("Reth node is ready");
                return Ok(());
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }

        Err(Error::Timeout(
            "Timed out waiting for Reth node to start".to_string(),
        ))
    }
}
