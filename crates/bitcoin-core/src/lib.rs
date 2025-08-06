//! Configures and runs the Bitcoin Core full node.
#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::Error;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use proven_bootable::Bootable;
use proven_isolation::{IsolatedApplication, IsolatedProcess, ReadyCheckInfo, VolumeMount};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;
use tracing::{debug, error, info};
use url::Url;

static P2P_PORT: u16 = 18333;

// Not sensitive
static RPC_USER: &str = "proven";
static RPC_PASSWORD: &str = "proven";

/// Regex pattern for matching Bitcoin Core log timestamps
static LOG_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\s+").unwrap());

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
    /// Optional path to the directory containing the bitcoind binary. If None, will use `which` to find it.
    pub bin_dir: Option<PathBuf>,

    /// The Bitcoin network to connect to.
    pub network: BitcoinNetwork,

    /// The directory to store data in.
    pub store_dir: PathBuf,

    /// RPC port
    pub rpc_port: u16,
}

/// Bitcoin Core application implementing the `IsolatedApplication` trait
struct BitcoinCoreApp {
    /// The directory containing the bitcoind executable
    bin_dir: PathBuf,

    /// The full path to the bitcoind executable
    executable_path: String,

    /// The Bitcoin network type
    network: BitcoinNetwork,

    /// RPC configuration
    rpc_port: u16,

    /// The directory to store data in
    store_dir: PathBuf,
}

#[async_trait]
impl IsolatedApplication for BitcoinCoreApp {
    fn args(&self) -> Vec<String> {
        let mut args = vec![
            format!("--datadir={}", self.store_dir.display()),
            "--server=1".to_string(),
            "--txindex=1".to_string(),
            "--listenonion=0".to_string(),
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
        }

        args
    }

    fn executable(&self) -> &str {
        &self.executable_path
    }

    fn handle_stderr(&self, line: &str) {
        let message = LOG_REGEX.replace(line, "").into_owned();
        error!(target: "bitcoind", "{}", message);
    }

    fn handle_stdout(&self, line: &str) {
        let message = LOG_REGEX.replace(line, "").into_owned();
        info!(target: "bitcoind", "{}", message);
    }

    fn name(&self) -> &'static str {
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

        let Ok(response) = client
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
        else {
            return false; // Not ready yet
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
            VolumeMount::new(&self.store_dir, &self.store_dir),
            VolumeMount::new(&self.bin_dir, &self.bin_dir),
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
    store_dir: PathBuf,

    /// The RPC port
    rpc_port: u16,

    /// The directory containing the bitcoind binary
    bin_dir: PathBuf,
}

impl BitcoinNode {
    /// Creates a new `BitcoinNode` with the specified options.
    ///
    /// # Errors
    ///
    /// Returns an error if the bitcoind binary is not found.
    ///
    /// # Panics
    ///
    /// Panics if the bitcoind binary is not found and `bin_dir` is not provided.
    pub fn new(options: BitcoinNodeOptions) -> Result<Self, Error> {
        let bin_dir = match options.bin_dir {
            Some(dir) => dir,
            None => match which::which("bitcoind") {
                Ok(path) => path
                    .parent()
                    .expect("which should have returned a path with a parent directory")
                    .to_path_buf(),
                Err(_) => return Err(Error::BinaryNotFound),
            },
        };

        Ok(Self {
            process: Arc::new(Mutex::new(None)),
            network: options.network,
            store_dir: options.store_dir,
            rpc_port: options.rpc_port,
            bin_dir,
        })
    }

    fn prepare_config(&self) -> Result<(), Error> {
        // Create the data directory if it doesn't exist
        let data_dir = Path::new(&self.store_dir);
        if !data_dir.exists() {
            std::fs::create_dir_all(data_dir)
                .map_err(|e| Error::Io("Failed to create data directory", e))?;
        }

        Ok(())
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
        #[derive(Deserialize)]
        struct RpcResponse<T> {
            result: T,
            error: Option<Value>,
        }

        let client = reqwest::Client::new();

        let response = client
            .post(self.rpc_url().await?)
            .basic_auth(RPC_USER, Some(RPC_PASSWORD))
            .json(&serde_json::json!({
                "jsonrpc": "1.0",
                "id": "proven",
                "method": method,
                "params": params
            }))
            .send()
            .await
            .map_err(|e| Error::RpcCall(format!("failed to send request: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response
                .text()
                .await
                .unwrap_or_else(|_| "could not read response body".to_string());

            return Err(Error::RpcCall(format!(
                "RPC call failed with status {status}: {text}"
            )));
        }

        let rpc_response: RpcResponse<R> = response
            .json()
            .await
            .map_err(|e| Error::RpcCall(format!("failed to parse response: {e}")))?;

        if let Some(error) = rpc_response.error {
            return Err(Error::RpcCall(format!("RPC error: {error:#?}")));
        }

        Ok(rpc_response.result)
    }

    /// Returns the RPC URL for the Bitcoin Core node.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL cannot be parsed.
    pub async fn rpc_url(&self) -> Result<Url, Error> {
        self.rpc_socket_addr().await.and_then(|socket_addr| {
            Url::parse(&format!("http://{}:{}", socket_addr.ip(), self.rpc_port))
                .map_err(Error::UrlParse)
        })
    }

    /// Returns the socket address of the Bitcoin Core node's RPC server.
    ///
    /// # Errors
    ///
    /// Returns an error if the node is not started.
    pub async fn rpc_socket_addr(&self) -> Result<SocketAddr, Error> {
        let process_guard = self.process.lock().await;
        let process = process_guard.as_ref().ok_or(Error::NotStarted)?;

        let ip = process
            .container_ip()
            .unwrap_or(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));

        drop(process_guard);

        Ok(SocketAddr::new(ip, self.rpc_port))
    }
}

#[async_trait]
impl Bootable for BitcoinNode {
    fn bootable_name(&self) -> &'static str {
        "bitcoin-core"
    }

    /// Starts the Bitcoin Core node in an isolated environment.
    ///
    /// # Errors
    ///
    /// Returns an error if the node fails to start.
    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.process.lock().await.is_some() {
            return Err(Box::new(Error::AlreadyStarted));
        }

        debug!("Starting isolated Bitcoin Core node...");

        self.prepare_config()?;

        let app = BitcoinCoreApp {
            bin_dir: self.bin_dir.clone(),
            executable_path: self.bin_dir.join("bitcoind").to_string_lossy().into_owned(),
            network: self.network,
            store_dir: self.store_dir.clone(),
            rpc_port: self.rpc_port,
        };

        let process = proven_isolation::spawn(app)
            .await
            .map_err(Error::Isolation)?;

        process.wait_until_ready().await;

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
    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Shutting down isolated Bitcoin Core node...");

        let taken_process = self.process.lock().await.take();
        if let Some(process) = taken_process {
            process
                .shutdown()
                .await
                .map_err(|e| Box::new(Error::Isolation(e)))?;

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
