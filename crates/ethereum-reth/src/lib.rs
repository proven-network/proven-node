//! Configures and runs the Reth Ethereum execution client.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::{Error, Result};

use std::fs;
use std::path::Path;
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

// RPC endpoints
static RETH_RPC_URL: &str = "http://127.0.0.1:8545";

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
    network: EthereumNetwork,
    shutdown_token: CancellationToken,
    store_dir: String,
    task_tracker: TaskTracker,
}

/// Options for configuring a `RethNode`.
pub struct RethNodeOptions {
    /// The Ethereum network to connect to.
    pub network: EthereumNetwork,

    /// The directory to store data in.
    pub store_dir: String,
}

impl RethNode {
    /// Create a new Reth node.
    #[must_use]
    pub fn new(RethNodeOptions { network, store_dir }: RethNodeOptions) -> Self {
        Self {
            network,
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
        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();
        let network = self.network;
        let store_dir = self.store_dir.clone();

        let server_task = self.task_tracker.spawn(async move {
            // Start the Reth process
            let data_dir = Path::new(&store_dir).join("reth");

            let mut cmd = Command::new("reth");
            cmd.args(["node", "--full", "--datadir", data_dir.to_str().unwrap()]);

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

            // Enable HTTP-RPC server with localhost restriction (no auth)
            cmd.args([
                "--http", // Enable HTTP-RPC server
                "--http.addr",
                "127.0.0.1", // Restrict to localhost
                "--http.api",
                "eth,net,web3,txpool", // Enable common APIs
            ]);

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
        let data_dir = Path::new(&self.store_dir);
        fs::create_dir_all(data_dir)?;
        Ok(())
    }

    async fn wait_until_ready(&self) -> Result<()> {
        // Simple health check - wait for RPC to be ready
        let client = Client::new();
        let url = format!("{}", RETH_RPC_URL);

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
