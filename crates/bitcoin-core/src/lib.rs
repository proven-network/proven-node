//! Configures and runs the Bitcoin Core full node.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::Error;
use proven_bootable::Bootable;
use url::Url;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use once_cell::sync::Lazy;
use proven_isolation::{IsolatedApplication, IsolatedProcess, ReadyCheckInfo, VolumeMount};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

static P2P_PORT: u16 = 18333;

// Not sensitive
static RPC_USER: &str = "proven";
static RPC_PASSWORD: &str = "proven";

/// Regex pattern for matching Bitcoin Core log timestamps
static TIMESTAMP_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\s+").unwrap());

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

/// Options for configuring an `BitcoinNode`.
pub struct BitcoinNodeOptions {
    /// The Bitcoin network to connect to.
    pub network: BitcoinNetwork,

    /// The directory to store data in.
    pub store_dir: String,

    /// Optional RPC port (defaults to 8332)
    pub rpc_port: Option<u16>,
}

/// Bitcoin Core application implementing the IsolatedApplication trait
struct BitcoinCoreApp {
    /// The path to the bitcoind executable
    executable_path: String,

    /// The Bitcoin network type
    network: BitcoinNetwork,

    /// RPC configuration
    rpc_port: u16,

    /// The directory to store data in
    store_dir: String,
}

#[async_trait]
impl IsolatedApplication for BitcoinCoreApp {
    fn args(&self) -> Vec<String> {
        let mut args = vec![
            "--datadir=/data".to_string(),
            "--server=1".to_string(),
            "--txindex=1".to_string(),
            "--rpcallowip=0.0.0.0/0".to_string(),
            "--rpcbind=0.0.0.0".to_string(),
            format!("--port={}", P2P_PORT),
            format!("--rpcport={}", self.rpc_port),
            format!("--rpcuser={}", RPC_USER),
            format!("--rpcpassword={}", RPC_PASSWORD),
        ];

        // Add network-specific arguments
        match self.network {
            BitcoinNetwork::Mainnet => {}
            BitcoinNetwork::Testnet => args.push("--testnet".to_string()),
            BitcoinNetwork::Regtest => args.push("--regtest".to_string()),
            BitcoinNetwork::Signet => args.push("--signet".to_string()),
        };

        args
    }

    fn executable(&self) -> &str {
        &self.executable_path
    }

    fn handle_stderr(&self, line: &str) {
        let message = TIMESTAMP_REGEX.replace(line, "").into_owned();
        error!(target: "bitcoind", "{}", message);
    }

    fn handle_stdout(&self, line: &str) {
        let message = TIMESTAMP_REGEX.replace(line, "").into_owned();
        info!(target: "bitcoind", "{}", message);
    }

    fn name(&self) -> &str {
        "bitcoind"
    }

    fn memory_limit_mb(&self) -> usize {
        // Bitcoin Core can be memory intensive, so allocate 8GB by default
        8192
    }

    async fn is_ready_check(&self, info: ReadyCheckInfo) -> bool {
        // To check if Bitcoin Core is ready, we'll use the RPC interface
        // to call the `getblockchaininfo` method
        let rpc_url = format!("http://{}:{}", info.ip_address, self.rpc_port);

        let client = reqwest::Client::new();

        let response = match client
            .post(&rpc_url)
            .basic_auth(RPC_USER, Some(RPC_PASSWORD))
            .json(&serde_json::json!({
                "jsonrpc": "1.0",
                "id": "proven",
                "method": "getblockchaininfo",
                "params": []
            }))
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(_) => return false, // Not ready yet
        };

        // If we get a 200 response, the node is up and running
        response.status().is_success()
    }

    fn is_ready_check_interval_ms(&self) -> u64 {
        // Check every 2 seconds
        2000
    }

    fn tcp_port_forwards(&self) -> Vec<u16> {
        vec![P2P_PORT]
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        vec![
            VolumeMount::new(&self.store_dir, &"/data".to_string()),
            VolumeMount::new("/apps/bitcoin/v28.1", "/apps/bitcoin/v28.1"),
        ]
    }
}

/// Represents an isolated Bitcoin Core node.
#[derive(Clone)]
pub struct BitcoinNode {
    /// The isolated process running Bitcoin Core
    process: Arc<Mutex<Option<IsolatedProcess>>>,

    /// The Bitcoin network type
    network: BitcoinNetwork,

    /// The directory to store data in
    store_dir: String,

    /// The RPC port
    rpc_port: u16,
}

impl BitcoinNode {
    /// Creates a new `BitcoinNode` with the specified options.
    #[must_use]
    pub fn new(options: BitcoinNodeOptions) -> Self {
        Self {
            process: Arc::new(Mutex::new(None)),
            network: options.network,
            store_dir: options.store_dir,
            rpc_port: options.rpc_port.unwrap_or(8332),
        }
    }

    /// Returns the RPC URL for the Bitcoin Core node.
    #[must_use]
    pub async fn get_rpc_url(&self) -> Result<Url, Error> {
        self.get_rpc_socket_addr().await.and_then(|socket_addr| {
            Url::parse(&format!("http://{}:{}", socket_addr.ip(), self.rpc_port))
                .map_err(Error::UrlParse)
        })
    }

    /// Returns the IP address of the Bitcoin Core node.
    #[must_use]
    pub async fn get_rpc_socket_addr(&self) -> Result<SocketAddr, Error> {
        self.process
            .lock()
            .await
            .as_ref()
            .ok_or(Error::NotStarted)?
            .container_ip()
            .map(|ip| SocketAddr::new(ip, self.rpc_port))
            .ok_or(Error::NotStarted)
    }

    /// Make an RPC call to the Bitcoin Core node
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC call fails.
    pub async fn rpc_call<T: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: T,
    ) -> Result<R, Error> {
        let client = reqwest::Client::new();

        let response = client
            .post(self.get_rpc_url().await?)
            .basic_auth(RPC_USER, Some(RPC_PASSWORD))
            .json(&serde_json::json!({
                "jsonrpc": "1.0",
                "id": "proven",
                "method": method,
                "params": params
            }))
            .send()
            .await
            .map_err(|e| Error::RpcCall(format!("failed to send request: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response
                .text()
                .await
                .unwrap_or_else(|_| "could not read response body".to_string());

            return Err(Error::RpcCall(format!(
                "RPC call failed with status {}: {}",
                status, text
            )));
        }

        #[derive(Deserialize)]
        struct RpcResponse<T> {
            result: T,
            error: Option<Value>,
        }

        let rpc_response: RpcResponse<R> = response
            .json()
            .await
            .map_err(|e| Error::RpcCall(format!("failed to parse response: {}", e)))?;

        if let Some(error) = rpc_response.error {
            return Err(Error::RpcCall(format!("RPC error: {:#?}", error)));
        }

        Ok(rpc_response.result)
    }

    async fn prepare_config(&self) -> Result<(), Error> {
        // Create the data directory if it doesn't exist
        let data_dir = Path::new(&self.store_dir);
        if !data_dir.exists() {
            std::fs::create_dir_all(data_dir)
                .map_err(|e| Error::Io("Failed to create data directory", e))?;
        }

        Ok(())
    }
}

#[async_trait]
impl Bootable for BitcoinNode {
    type Error = Error;

    /// Starts the Bitcoin Core node in an isolated environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the node fails to start.
    async fn start(&self) -> Result<(), Error> {
        if self.process.lock().await.is_some() {
            return Err(Error::AlreadyStarted);
        }

        debug!("Starting isolated Bitcoin Core node...");

        self.prepare_config().await?;

        let app = BitcoinCoreApp {
            executable_path: "/apps/bitcoin/v28.1/bitcoind".to_string(),
            network: self.network,
            store_dir: self.store_dir.clone(),
            rpc_port: self.rpc_port,
        };

        let process = proven_isolation::spawn(app)
            .await
            .map_err(Error::Isolation)?;

        // Store the process for later shutdown
        self.process.lock().await.replace(process);

        info!("Bitcoin Core node started");

        Ok(())
    }

    /// Shuts down the Bitcoin Core node.
    ///
    /// # Errors
    ///
    /// Returns an error if the node fails to shutdown.
    async fn shutdown(&self) -> Result<(), Error> {
        info!("Shutting down isolated Bitcoin Core node...");

        if let Some(process) = self.process.lock().await.take() {
            process.shutdown().await.map_err(Error::Isolation)?;
            info!("Bitcoin Core node shut down successfully");
        } else {
            debug!("No running Bitcoin Core node to shut down");
        }

        Ok(())
    }

    async fn wait(&self) {
        if let Some(process) = self.process.lock().await.as_ref() {
            process.wait().await;
        }
    }
}
