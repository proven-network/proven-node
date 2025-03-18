//! Configures and runs the Bitcoin Core full node.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::{Error, Result};

use std::fs;
use std::process::Stdio;
use std::str;

use nix::sys::signal;
use nix::sys::signal::Signal;
use nix::unistd::Pid;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

// RPC endpoint (host/port)
static BITCOIND_RPC_HOST: &str = "127.0.0.1";
static BITCOIND_RPC_PORT: u16 = 8332;
static BITCOIND_RPC_USER: &str = "proven";
static BITCOIND_RPC_PASSWORD: &str = "proven";

/// Represents a Bitcoin network
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitcoinNetwork {
    /// Bitcoin mainnet
    Mainnet,
    /// Bitcoin testnet
    Testnet,
    /// Bitcoin regtest (for local testing)
    Regtest,
    /// Bitcoin signet (newer testnet)
    Signet,
}

impl BitcoinNetwork {
    /// Returns the network name as a string
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Testnet => "testnet",
            Self::Regtest => "regtest",
            Self::Signet => "signet",
        }
    }
}

/// Runs a Bitcoin Core node.
pub struct BitcoinNode {
    network: BitcoinNetwork,
    shutdown_token: CancellationToken,
    store_dir: String,
    task_tracker: TaskTracker,
}

/// Options for configuring a `BitcoinNode`.
pub struct BitcoinNodeOptions {
    /// The Bitcoin network to connect to.
    pub network: BitcoinNetwork,

    /// The directory to store data in.
    pub store_dir: String,
}

impl BitcoinNode {
    /// Creates a new `BitcoinNode`.
    #[must_use]
    pub fn new(BitcoinNodeOptions { network, store_dir }: BitcoinNodeOptions) -> Self {
        Self {
            network,
            shutdown_token: CancellationToken::new(),
            store_dir,
            task_tracker: TaskTracker::new(),
        }
    }

    /// Starts the Bitcoin Core node.
    ///
    /// # Errors
    ///
    /// Returns an error if the node fails to start.
    #[allow(clippy::too_many_lines)]
    pub async fn start(&self) -> Result<JoinHandle<()>> {
        debug!("Starting Bitcoin Core node...");

        fs::create_dir_all(&self.store_dir)
            .map_err(|e| Error::Io("failed to create data directory", e))?;

        // Start bitcoind
        let network_arg = match self.network {
            BitcoinNetwork::Mainnet => vec![],
            BitcoinNetwork::Testnet => vec!["--testnet"],
            BitcoinNetwork::Regtest => vec!["--regtest"],
            BitcoinNetwork::Signet => vec!["--signet"],
        };

        let mut cmd = Command::new("bitcoind");
        cmd.args([&format!("--datadir={}", self.store_dir)])
            .args(network_arg)
            .args([
                "--server=1",
                "--txindex=1",
                &format!("--rpcbind={}", BITCOIND_RPC_HOST),
                &format!("--rpcport={}", BITCOIND_RPC_PORT),
                &format!("--rpcuser={}", BITCOIND_RPC_USER),
                &format!("--rpcpassword={}", BITCOIND_RPC_PASSWORD),
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        debug!("Attempting to spawn bitcoind with command: {:?}", cmd);
        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => {
                error!("Failed to spawn bitcoind process: {}", e);
                error!("Command was: {:?}", cmd);
                return Err(Error::Io("failed to spawn bitcoind", e));
            }
        };
        info!("Bitcoin Core process spawned successfully");

        // Handle stdout
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| Error::StartBitcoind("failed to get stdout".to_string()))?;
        let stdout_reader = BufReader::new(stdout);
        let stdout_lines = stdout_reader.lines();

        let tracker = self.task_tracker.clone();
        let _stdout_task = tracker.spawn(async move {
            let mut lines = stdout_lines;
            while let Ok(Some(line)) = lines.next_line().await {
                info!("bitcoind stdout: {line}");
            }
        });

        // Handle stderr
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| Error::StartBitcoind("failed to get stderr".to_string()))?;
        let stderr_reader = BufReader::new(stderr);
        let stderr_lines = stderr_reader.lines();

        let tracker = self.task_tracker.clone();
        let _stderr_task = tracker.spawn(async move {
            let mut lines = stderr_lines;
            while let Ok(Some(line)) = lines.next_line().await {
                info!("bitcoind stderr: {line}");
            }
        });

        // Monitor child process
        let shutdown_token = self.shutdown_token.clone();
        let main_task = self.task_tracker.spawn(async move {
            tokio::select! {
                status = child.wait() => {
                    match status {
                        Ok(status) => {
                            if status.success() {
                                info!("bitcoind exited with status: {status}");
                            } else {
                                error!("bitcoind exited with non-zero status: {status}");
                            }
                        }
                        Err(err) => {
                            error!("failed to wait for bitcoind: {err}");
                        }
                    }
                }
                () = shutdown_token.cancelled() => {
                    info!("Stopping bitcoind...");
                    // Try to stop bitcoind gracefully with SIGTERM

                    if let Some(id) = child.id() {
                        let Ok(raw_pid) = id.try_into() else {
                            error!("Failed to convert process ID");
                            return;
                        };

                        let pid = Pid::from_raw(raw_pid);
                        if let Err(err) = signal::kill(pid, Signal::SIGTERM) {
                            error!("failed to send SIGTERM to bitcoind: {err}");
                        }
                    }

                    // Wait for the process to exit
                    if let Ok(result) = tokio::time::timeout(tokio::time::Duration::from_secs(30), child.wait()).await {
                        match result {
                            Ok(status) => {
                                info!("bitcoind exited with status: {status}");
                            }
                            Err(err) => {
                                error!("failed to wait for bitcoind: {err}");
                            }
                        }
                    } else {
                        error!("timeout waiting for bitcoind to exit, killing...");
                        if let Err(err) = child.kill().await {
                            error!("failed to kill bitcoind: {err}");
                        }
                    }
                }
            }
        });

        self.task_tracker.close();

        // Wait for the node to be ready
        self.wait_until_ready().await?;

        Ok(main_task)
    }

    /// Shuts down the Bitcoin Core node.
    pub async fn shutdown(&self) {
        info!("Shutting down Bitcoin Core node...");

        // Trigger cancellation
        self.shutdown_token.cancel();

        // Wait for tasks to complete
        self.task_tracker.wait().await;

        info!("Bitcoin Core node shut down.");
    }

    /// Returns the RPC URL for the Bitcoin Core node.
    #[must_use]
    pub fn get_rpc_url(&self) -> String {
        format!("http://{BITCOIND_RPC_HOST}:{BITCOIND_RPC_PORT}")
    }

    /// Make an RPC call to the Bitcoin Core node
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC call fails
    pub async fn rpc_call<T: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: T,
    ) -> Result<R> {
        let client = reqwest::Client::new();

        let request_body = json!({
            "jsonrpc": "1.0",
            "id": "rust-client",
            "method": method,
            "params": params,
        });

        let response = client
            .post(&self.get_rpc_url())
            .basic_auth(BITCOIND_RPC_USER, Some(BITCOIND_RPC_PASSWORD))
            .json(&request_body)
            .send()
            .await
            .map_err(|e| Error::Rpc(format!("Request failed: {}", e)))?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(Error::Rpc(format!("HTTP error {}: {}", status, error_text)));
        }

        let response_body: serde_json::Value = response
            .json()
            .await
            .map_err(|e| Error::Rpc(format!("Failed to parse response: {}", e)))?;

        if let Some(error) = response_body.get("error") {
            if !error.is_null() {
                return Err(Error::Rpc(format!("RPC error: {:?}", error)));
            }
        }

        serde_json::from_value(response_body["result"].clone())
            .map_err(|e| Error::Rpc(format!("Failed to parse result: {}", e)))
    }

    async fn wait_until_ready(&self) -> Result<()> {
        info!("Waiting for Bitcoin Core node to be ready...");

        let mut retries = 0;
        let max_retries = 30;

        while retries < max_retries {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

            // Try to make a simple RPC call
            match self
                .rpc_call::<Vec<()>, Value>("getblockchaininfo", vec![])
                .await
            {
                Ok(_) => {
                    info!("Bitcoin Core node is ready with RPC interface accessible.");
                    return Ok(());
                }
                Err(e) => {
                    debug!("Node not ready yet: {}", e);
                }
            }

            retries += 1;
            debug!("Waiting for Bitcoin Core node to be ready ({retries}/{max_retries})");
        }

        error!("Timed out waiting for Bitcoin Core node to be ready");
        Err(Error::NotReady)
    }
}
