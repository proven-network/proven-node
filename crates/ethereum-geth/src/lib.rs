//! Configures and runs the Geth Ethereum execution client.
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

use reqwest::Client;
use nix::sys::signal;
use nix::sys::signal::Signal;
use nix::unistd::Pid;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, trace};

// Configuration paths
#[allow(dead_code)]
static GETH_DATA_DIR: &str = "/var/lib/proven-node/ethereum/geth";
#[allow(dead_code)]
static GETH_CONFIG_PATH: &str = "/var/lib/proven-node/ethereum/geth.config";

// RPC endpoints
static GETH_RPC_URL: &str = "http://127.0.0.1:8545";

/// Represents an Ethereum network
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EthereumNetwork {
    /// Ethereum mainnet
    Mainnet,
    /// Ethereum testnet (Sepolia)
    Sepolia,
    /// Ethereum testnet (Goerli)
    Goerli,
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
            Self::Goerli => "goerli",
            Self::Holesky => "holesky",
        }
    }
}

/// Runs a Geth execution client.
pub struct GethNode {
    network: EthereumNetwork,
    shutdown_token: CancellationToken,
    store_dir: String,
    task_tracker: TaskTracker,
}

/// Options for configuring a `GethNode`.
pub struct GethNodeOptions {
    /// The Ethereum network to connect to.
    pub network: EthereumNetwork,

    /// The directory to store data in.
    pub store_dir: String,
}

impl GethNode {
    /// Creates a new instance of `GethNode`.
    #[must_use]
    pub fn new(GethNodeOptions { network, store_dir }: GethNodeOptions) -> Self {
        Self {
            network,
            shutdown_token: CancellationToken::new(),
            store_dir,
            task_tracker: TaskTracker::new(),
        }
    }

    /// Starts the Geth execution client.
    ///
    /// This function spawns a new Geth process, sets up logging for stdout and stderr,
    /// and waits until the Geth RPC endpoint is ready.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - Failed to create required data directories
    /// - Geth RPC endpoint doesn't become available within the timeout period
    /// - The process fails to start or exits with a non-zero status
    #[allow(clippy::too_many_lines)]
    pub async fn start(&self) -> Result<JoinHandle<()>> {
        // Ensure data directories exist
        self.create_data_directories()?;

        // Start Geth and handle it in a single task
        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();
        let network = self.network;
        let store_dir = self.store_dir.clone();

        let server_task = self.task_tracker.spawn(async move {
            // Start the Geth process
            let network_str = network.as_str();
            let data_dir = Path::new(&store_dir).join("geth");

            let mut cmd = Command::new("/bin/geth");
            cmd.args(["--datadir", data_dir.to_str().unwrap_or(GETH_DATA_DIR)]);

            // Add network-specific args
            if network != EthereumNetwork::Mainnet {
                cmd.arg(format!("--{network_str}"));
            }

            cmd.stdout(Stdio::piped());
            cmd.stderr(Stdio::piped());

            let mut child = match cmd.spawn() {
                Ok(child) => child,
                Err(e) => {
                    error!("Failed to spawn geth: {}", e);
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
                        trace!(target: "geth", "{s}");
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
                        trace!(target: "geth", "{s}");
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
                            error!("Failed to wait for geth: {}", e);
                            return;
                        }
                    };

                    // Clean up the stdout/stderr tasks
                    geth_stdout_task.abort();
                    geth_stderr_task.abort();

                    if !status.success() {
                        error!("Geth exited with non-zero status: {}", status);
                        return;
                    }

                    info!("Geth process completed successfully");
                }
                () = shutdown_token.cancelled() => {
                    info!("Shutdown requested, terminating Geth process");

                    // Get the PID and send SIGTERM
                    if let Some(id) = child.id() {
                        let Ok(raw_pid) = id.try_into() else {
                            error!("Failed to convert process ID");
                            return;
                        };

                        let pid = Pid::from_raw(raw_pid);
                        if let Err(e) = signal::kill(pid, Signal::SIGTERM) {
                            error!("Failed to send SIGTERM to Geth process: {}", e);
                        }
                    } else {
                        error!("Could not get Geth process ID for termination");
                    }

                    // Wait for the process to exit
                    let _ = child.wait().await;

                    // Clean up the stdout/stderr tasks
                    geth_stdout_task.abort();
                    geth_stderr_task.abort();

                    info!("Geth process terminated");
                }
            }
        });

        // Wait until Geth is ready before returning
        self.wait_until_ready().await?;

        Ok(server_task)
    }

    /// Shuts down the Geth node.
    pub async fn shutdown(&self) {
        info!("Geth node shutting down...");
        self.shutdown_token.cancel();
        self.task_tracker.wait().await;
        info!("Geth node shutdown");
    }

    /// Creates necessary data directories for the Geth node.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - Failed to create the geth directory due to filesystem permissions or other IO errors
    fn create_data_directories(&self) -> Result<()> {
        let geth_dir = Path::new(&self.store_dir).join("geth");

        if !geth_dir.exists() {
            fs::create_dir_all(&geth_dir)
                .map_err(|e| Error::Io("failed to create geth directory", e))?;
        }

        Ok(())
    }

    /// Waits until the Geth node is ready by polling the RPC endpoint.
    ///
    /// The function attempts to connect to the Geth RPC endpoint for up to 30 attempts,
    /// with a 2-second delay between attempts.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The Geth RPC endpoint does not become available within 60 seconds
    /// - HTTP requests to the RPC endpoint consistently fail
    async fn wait_until_ready(&self) -> Result<()> {
        // Create HTTP client
        let client = Client::new();

        // Check Geth RPC endpoint until it responds
        let mut geth_ready = false;
        for _ in 0..30 {
            // Test Geth JSON-RPC
            let body = r#"{"jsonrpc":"2.0","method":"web3_clientVersion","params":[],"id":1}"#;

            match client
                .post(GETH_RPC_URL)
                .header("Content-Type", "application/json")
                .body(body)
                .send()
                .await
            {
                Ok(response) => {
                    if response.status() == 200 {
                        info!("Geth is ready");
                        geth_ready = true;
                        break;
                    }
                }
                Err(e) => debug!("Geth not ready yet: {e}"),
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        }

        if !geth_ready {
            return Err(Error::HttpRequest(
                "Geth RPC endpoint not available".to_string(),
            ));
        }

        info!("Geth execution client is ready");
        Ok(())
    }
}
