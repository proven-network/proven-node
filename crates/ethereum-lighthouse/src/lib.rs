//! Configures and runs the Lighthouse Ethereum consensus client.
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
use serde_json;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

// Configuration paths
#[allow(dead_code)]
static LIGHTHOUSE_DATA_DIR: &str = "/var/lib/proven-node/ethereum/lighthouse";
#[allow(dead_code)]
static LIGHTHOUSE_CONFIG_PATH: &str = "/var/lib/proven-node/ethereum/lighthouse.config";

// RPC endpoints
static LIGHTHOUSE_HTTP_URL: &str = "http://127.0.0.1:5052";

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
    #[allow(dead_code)]
    host_ip: String,
    network: EthereumNetwork,
    shutdown_token: CancellationToken,
    store_dir: String,
    task_tracker: TaskTracker,
}

/// Options for configuring a `LighthouseNode`.
pub struct LighthouseNodeOptions {
    /// The host IP address.
    pub host_ip: String,

    /// The Ethereum network to connect to.
    pub network: EthereumNetwork,

    /// The directory to store data in.
    pub store_dir: String,
}

impl LighthouseNode {
    /// Creates a new instance of `LighthouseNode`.
    #[must_use]
    pub fn new(
        LighthouseNodeOptions {
            host_ip,
            network,
            store_dir,
        }: LighthouseNodeOptions,
    ) -> Self {
        Self {
            host_ip,
            network,
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
        let shutdown_token = self.shutdown_token.clone();
        let task_tracker = self.task_tracker.clone();
        let network = self.network;
        let store_dir = self.store_dir.clone();

        let server_task = self.task_tracker.spawn(async move {
            // Start the Lighthouse process
            let network_str = network.as_str();
            let data_dir = Path::new(&store_dir).join("lighthouse");
            let jwt_path = format!("/tmp/proven/ethereum-{}/geth/jwtsecret", network_str);

            let mut cmd = Command::new("lighthouse");
            cmd.args([
                "beacon_node",
                "--datadir",
                data_dir.to_str().unwrap_or(LIGHTHOUSE_DATA_DIR),
                "--network",
                network_str,
                "--execution-endpoint",
                "http://127.0.0.1:8551",
                "--http",
                "--http-address",
                "127.0.0.1",
                "--http-port",
                "5052",
                "--metrics",
                "--metrics-address",
                "127.0.0.1",
                "--metrics-port",
                "5054",
                "--checkpoint-sync-url",
                &get_checkpoint_sync_url(network),
                "--jwt-secrets",
                &jwt_path,
                "--purge-db",
            ]);

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
                        if let Err(e) = signal::kill(pid, Signal::SIGTERM) {
                            error!("Failed to send SIGTERM to Lighthouse process: {}", e);
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
        let lighthouse_dir = Path::new(&self.store_dir).join("lighthouse");

        if !lighthouse_dir.exists() {
            fs::create_dir_all(&lighthouse_dir)
                .map_err(|e| Error::Io("failed to create lighthouse directory", e))?;
        }

        Ok(())
    }

    // Waits until Lighthouse is ready
    async fn wait_until_ready(&self) -> Result<()> {
        // Create HTTP client
        let client = Client::new();

        // Check Lighthouse HTTP endpoint until it responds
        info!(
            "Waiting for Lighthouse HTTP API to become available and syncing to complete at {LIGHTHOUSE_HTTP_URL}/lighthouse/syncing"
        );

        // Keep trying indefinitely as long as we're syncing
        let mut attempt = 1;
        loop {
            info!("HTTP syncing check attempt {attempt}");
            match client
                .get(format!("{LIGHTHOUSE_HTTP_URL}/lighthouse/syncing"))
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
        EthereumNetwork::Mainnet => "https://mainnet.beaconstate.info/".to_string(),
        EthereumNetwork::Sepolia => "https://sepolia.beaconstate.info".to_string(),
        EthereumNetwork::Holesky => "https://holesky.beaconstate.info".to_string(),
    }
}
