//! Configures and runs the Lighthouse Ethereum consensus client.
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
use serde_json;
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

/// Runs a Lighthouse consensus client.
pub struct LighthouseNode {
    execution_rpc_addr: SocketAddrV4,
    host_ip: String,
    http_addr: SocketAddrV4,
    metrics_addr: SocketAddrV4,
    network: EthereumNetwork,
    p2p_addr: SocketAddrV4,
    shutdown_token: CancellationToken,
    store_dir: PathBuf,
    task_tracker: TaskTracker,
}

/// Options for configuring a `LighthouseNode`.
pub struct LighthouseNodeOptions {
    /// The execution client RPC endpoint.
    pub execution_rpc_addr: SocketAddrV4,

    /// The host IP address.
    pub host_ip: String,

    /// The HTTP API socket address.
    pub http_addr: SocketAddrV4,

    /// The metrics socket address.
    pub metrics_addr: SocketAddrV4,

    /// The Ethereum network to connect to.
    pub network: EthereumNetwork,

    /// The P2P networking socket address.
    pub p2p_addr: SocketAddrV4,

    /// The directory to store data in.
    pub store_dir: PathBuf,
}

impl LighthouseNode {
    /// Creates a new instance of `LighthouseNode`.
    #[must_use]
    pub fn new(
        LighthouseNodeOptions {
            execution_rpc_addr,
            host_ip,
            http_addr,
            metrics_addr,
            network,
            p2p_addr,
            store_dir,
        }: LighthouseNodeOptions,
    ) -> Self {
        Self {
            execution_rpc_addr,
            host_ip,
            http_addr,
            metrics_addr,
            network,
            p2p_addr,
            shutdown_token: CancellationToken::new(),
            store_dir,
            task_tracker: TaskTracker::new(),
        }
    }

    /// Starts the Lighthouse consensus client.
    ///
    /// # Errors
    /// Returns an error if:
    /// - Data directories cannot be created
    /// - The Lighthouse process fails to start
    /// - The Lighthouse HTTP endpoint fails to become available
    #[allow(clippy::too_many_lines)]
    pub async fn start(&self) -> Result<JoinHandle<()>> {
        // Ensure data directories exist
        self.create_data_directories()?;

        // Start Lighthouse and handle it in a single task
        let execution_rpc_addr = self.execution_rpc_addr;
        let host_ip = self.host_ip.clone();
        let http_addr = self.http_addr;
        let metrics_addr = self.metrics_addr;
        let network = self.network;
        let p2p_addr = self.p2p_addr;
        let shutdown_token = self.shutdown_token.clone();
        let store_dir = self.store_dir.clone();
        let task_tracker = self.task_tracker.clone();

        let server_task = self.task_tracker.spawn(async move {
            // Start the Lighthouse process
            let network_str = network.as_str();
            let jwt_path = format!("/tmp/proven/ethereum-{}/reth/jwt.hex", network_str);

            let mut cmd = Command::new("lighthouse");
            cmd.args(["beacon_node", "--datadir", store_dir.to_str().unwrap()]);

            // Add network-specific args
            match network {
                EthereumNetwork::Mainnet => {
                    // Mainnet is default, no args needed
                }
                EthereumNetwork::Sepolia => {
                    cmd.arg("--network=sepolia");
                }
                EthereumNetwork::Holesky => {
                    cmd.arg("--network=holesky");
                }
            }

            // Add execution endpoint
            cmd.args([
                "--execution-endpoint",
                &format!(
                    "http://{}:{}",
                    execution_rpc_addr.ip(),
                    execution_rpc_addr.port()
                ),
            ]);

            // Add JWT secret
            cmd.args(["--jwt-secrets", &jwt_path]);

            // Add P2P networking args
            cmd.args([
                "--listen-address",
                &p2p_addr.ip().to_string(),
                "--port",
                &p2p_addr.port().to_string(),
            ]);

            // Add HTTP API args
            cmd.args([
                "--http",
                "--http-address",
                &http_addr.ip().to_string(),
                "--http-port",
                &http_addr.port().to_string(),
            ]);

            // Add metrics args
            cmd.args([
                "--metrics",
                "--metrics-address",
                &metrics_addr.ip().to_string(),
                "--metrics-port",
                &metrics_addr.port().to_string(),
            ]);

            // Add checkpoint sync URL
            cmd.args(["--checkpoint-sync-url", &get_checkpoint_sync_url(network)]);

            // Add ENR IP address
            cmd.args(["--enr-address", &host_ip]);

            // Disable UPNP
            cmd.args(["--disable-upnp"]);

            info!("Starting Lighthouse with command: {:?}", cmd);

            cmd.stdout(Stdio::piped());
            cmd.stderr(Stdio::piped());

            let mut child = match cmd.spawn() {
                Ok(child) => child,
                Err(e) => {
                    error!("Failed to spawn lighthouse: {}", e);
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
            let lh_stdout_task = task_tracker.spawn(async move {
                let mut lines = stdout_reader;
                while let Ok(Some(line)) = lines.next_line().await {
                    // Removing ANSI escape codes for cleaner logs
                    let bytes = strip_ansi_escapes::strip(&line);
                    // Skip logging if we don't have valid UTF-8
                    if let Ok(s) = std::str::from_utf8(&bytes) {
                        info!(target: "lighthouse", "{s}");
                    }
                }
            });

            let lh_stderr_task = task_tracker.spawn(async move {
                let mut lines = stderr_reader;
                while let Ok(Some(line)) = lines.next_line().await {
                    // Removing ANSI escape codes for cleaner logs
                    let bytes = strip_ansi_escapes::strip(&line);
                    // Skip logging if we don't have valid UTF-8
                    if let Ok(s) = std::str::from_utf8(&bytes) {
                        info!(target: "lighthouse", "{s}");
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
                            error!("Failed to wait for lighthouse: {}", e);
                            return;
                        }
                    };

                    // Clean up the stdout/stderr tasks
                    lh_stdout_task.abort();
                    lh_stderr_task.abort();

                    if !status.success() {
                        error!("Lighthouse exited with non-zero status: {}", status);
                        return;
                    }

                    info!("Lighthouse process completed successfully");
                }
                () = shutdown_token.cancelled() => {
                    info!("Shutdown requested, terminating Lighthouse process");

                    // Get the PID and send SIGTERM
                    if let Some(id) = child.id() {
                        let Ok(raw_pid) = id.try_into() else {
                            error!("Failed to convert process ID");
                            return;
                        };

                        let pid = Pid::from_raw(raw_pid);
                        if let Err(e) = signal::kill(pid, Signal::SIGINT) {
                            error!("Failed to send SIGINT to Lighthouse process: {}", e);
                        }
                    } else {
                        error!("Could not get Lighthouse process ID for termination");
                    }

                    // Wait for the process to exit
                    let _ = child.wait().await;

                    // Clean up the stdout/stderr tasks
                    lh_stdout_task.abort();
                    lh_stderr_task.abort();

                    info!("Lighthouse process terminated");
                }
            }
        });

        self.task_tracker.close();

        // Wait until Lighthouse is ready before returning
        self.wait_until_ready().await?;

        Ok(server_task)
    }

    /// Shuts down the Lighthouse node.
    pub async fn shutdown(&self) {
        info!("Lighthouse node shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("Lighthouse node shutdown");
    }

    // Creates necessary data directories
    fn create_data_directories(&self) -> Result<()> {
        if !self.store_dir.exists() {
            fs::create_dir_all(&self.store_dir)
                .map_err(|e| Error::Io("failed to create lighthouse directory", e))?;
        }

        Ok(())
    }

    // Waits until Lighthouse is ready
    async fn wait_until_ready(&self) -> Result<()> {
        // Create HTTP client
        let client = Client::new();
        let http_url = format!("http://{}:{}", self.http_addr.ip(), self.http_addr.port());

        // Check Lighthouse HTTP endpoint until it responds
        info!(
            "Waiting for Lighthouse HTTP API to become available and syncing to complete at {}/lighthouse/syncing",
            http_url
        );

        // Keep trying indefinitely as long as we're syncing
        let mut attempt = 1;
        loop {
            info!("HTTP syncing check attempt {attempt}");
            match client
                .get(format!("{}/lighthouse/syncing", http_url))
                .send()
                .await
            {
                Ok(response) => {
                    if response.status() == 200 {
                        // Parse the response to check if still syncing
                        match response.json::<serde_json::Value>().await {
                            Ok(json) => {
                                // Check if the node is still syncing
                                if let Some(data) = json.get("data") {
                                    if data.is_string() {
                                        match data.as_str() {
                                            Some("Synced") => {
                                                info!("Lighthouse syncing completed");
                                                info!(
                                                    "Lighthouse consensus client is ready and synced"
                                                );
                                                return Ok(());
                                            }
                                            Some("Stalled") => {
                                                info!(
                                                    "Lighthouse sync is stalled (waiting for peers), continuing to wait..."
                                                );
                                                tokio::time::sleep(
                                                    tokio::time::Duration::from_secs(10),
                                                )
                                                .await;
                                                attempt += 1;
                                                continue;
                                            }
                                            _ => {
                                                info!(
                                                    "Unexpected syncing response string: {:?}, will try again",
                                                    data
                                                );
                                            }
                                        }
                                    } else if data.is_object()
                                        && data.get("SyncingFinalized").is_some()
                                    {
                                        info!("Lighthouse is still syncing, continuing to wait...");
                                        tokio::time::sleep(tokio::time::Duration::from_secs(10))
                                            .await;
                                        attempt += 1;
                                        continue;
                                    } else {
                                        info!(
                                            "Unexpected syncing response format: {:?}, will try again",
                                            data
                                        );
                                    }
                                } else {
                                    info!(
                                        "Unexpected syncing response format, missing 'data' field"
                                    );
                                }
                            }
                            Err(e) => {
                                info!("Failed to parse syncing response: {e}, will try again")
                            }
                        }
                    } else {
                        info!(
                            "Lighthouse HTTP endpoint responded with status: {}",
                            response.status()
                        );
                    }
                }
                Err(e) => {
                    info!("Lighthouse HTTP endpoint not ready yet: {e}");
                    // Only return error if connection is refused or another critical error
                    if e.is_connect() || e.is_timeout() {
                        // This is likely because the service isn't up yet, keep trying
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        attempt += 1;
                        continue;
                    } else {
                        // Other types of errors might indicate a more serious problem
                        error!("Error connecting to Lighthouse: {e}");
                        return Err(Error::HttpRequest(format!("Lighthouse HTTP error: {e}")));
                    }
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            attempt += 1;
        }
    }
}

// Returns the appropriate checkpoint sync URL based on the network
fn get_checkpoint_sync_url(network: EthereumNetwork) -> String {
    match network {
        EthereumNetwork::Mainnet => "https://beaconstate-mainnet.chainsafe.io".to_string(),
        EthereumNetwork::Sepolia => "https://beaconstate-sepolia.chainsafe.io".to_string(),
        EthereumNetwork::Holesky => "https://beaconstate-holesky.chainsafe.io".to_string(),
    }
}
