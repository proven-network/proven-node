//! Configures and runs the Reth Ethereum execution client.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::Error;

use std::fs;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::{error::Error as StdError, net::IpAddr};

use async_trait::async_trait;
use proven_isolation::{IsolatedApplication, IsolatedProcess, VolumeMount};
use reqwest::Client;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

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

/// Options for configuring a `RethNode`.
pub struct RethNodeOptions {
    /// The peer discovery socket address.
    pub discovery_port: u16,

    /// The HTTP RPC socket address.
    pub http_port: u16,

    /// The metrics socket address.
    pub metrics_port: u16,

    /// The Ethereum network to connect to.
    pub network: EthereumNetwork,

    /// The RPC socket address.
    pub rpc_port: u16,

    /// The directory to store data in.
    pub store_dir: PathBuf,
}

/// Reth application implementing the IsolatedApplication trait
struct RethApp {
    /// The path to the reth executable
    executable_path: String,

    /// The peer discovery port
    discovery_port: u16,

    /// The HTTP RPC port
    http_port: u16,

    /// The metrics port
    metrics_port: u16,

    /// The Ethereum network type
    network: EthereumNetwork,

    /// The RPC port
    rpc_port: u16,

    /// The directory to store data in
    store_dir: PathBuf,
}

#[async_trait]
impl IsolatedApplication for RethApp {
    fn args(&self) -> Vec<String> {
        let mut args = vec!["node".to_string(), "--full".to_string()];

        // Add data directory
        args.push("--datadir=/data".to_string());

        // Add network-specific args
        match self.network {
            EthereumNetwork::Mainnet => {}
            EthereumNetwork::Sepolia => {
                args.push("--chain=sepolia".to_string());
            }
            EthereumNetwork::Holesky => {
                args.push("--chain=holesky".to_string());
            }
        }

        // Configure Auth server
        args.extend([
            "--authrpc.addr".to_string(),
            "0.0.0.0".to_string(),
            "--authrpc.port".to_string(),
            self.rpc_port.to_string(),
        ]);

        // Enable HTTP-RPC server with configured address
        args.extend([
            "--http".to_string(),
            "--http.addr".to_string(),
            "0.0.0.0".to_string(),
            "--http.port".to_string(),
            self.http_port.to_string(),
            "--http.api".to_string(),
            "eth,net,web3,txpool".to_string(),
        ]);

        // Configure peer discovery
        args.extend([
            "--discovery.addr".to_string(),
            "0.0.0.0".to_string(),
            "--discovery.port".to_string(),
            self.discovery_port.to_string(),
        ]);

        // Configure metrics
        args.extend([
            "--metrics".to_string(),
            format!("0.0.0.0:{}", self.metrics_port),
        ]);

        args
    }

    fn executable(&self) -> &str {
        &self.executable_path
    }

    fn handle_stderr(&self, line: &str) {
        error!(target: "reth", "{}", line);
    }

    fn handle_stdout(&self, line: &str) {
        info!(target: "reth", "{}", line);
    }

    fn name(&self) -> &str {
        "reth"
    }

    fn memory_limit_mb(&self) -> usize {
        // Reth can be memory intensive, allocate 8GB by default
        8192
    }

    async fn is_ready_check(&self, process: &IsolatedProcess) -> Result<bool, Box<dyn StdError>> {
        let http_url = if let Some(ip) = process.container_ip() {
            format!("http://{}:{}", ip, self.http_port)
        } else {
            format!("http://127.0.0.1:{}", self.http_port)
        };

        let client = Client::new();
        match client
            .post(&http_url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "eth_syncing",
                "params": [],
                "id": 1
            }))
            .send()
            .await
        {
            Ok(response) => {
                if response.status() == 200 {
                    match response.json::<serde_json::Value>().await {
                        Ok(json) => {
                            // If result is false, the node is synced
                            // If result is an object, the node is still syncing
                            if let Some(result) = json.get("result") {
                                if result.is_boolean() {
                                    return Ok(!result.as_bool().unwrap_or(true));
                                }
                            }
                        }
                        Err(_) => return Ok(false),
                    }
                }
                Ok(false)
            }
            Err(_) => Ok(false),
        }
    }

    fn is_ready_check_interval_ms(&self) -> u64 {
        // Check every 10 seconds
        10000
    }

    fn tcp_port_forwards(&self) -> Vec<u16> {
        vec![
            self.discovery_port,
            self.http_port,
            self.metrics_port,
            self.rpc_port,
        ]
    }

    fn udp_port_forwards(&self) -> Vec<u16> {
        vec![self.discovery_port]
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        vec![
            VolumeMount::new(self.store_dir.to_str().unwrap(), "/data"),
            VolumeMount::new("/apps/ethereum-reth/v1.3.8", "/apps/ethereum-reth/v1.3.8"),
        ]
    }
}

/// Runs a Reth execution client.
pub struct RethNode {
    /// The isolated process running Reth
    process: Option<IsolatedProcess>,

    /// The peer discovery port
    discovery_port: u16,

    /// The HTTP RPC port
    http_port: u16,

    /// The metrics port
    metrics_port: u16,

    /// The Ethereum network type
    network: EthereumNetwork,

    /// The RPC port
    rpc_port: u16,

    /// The directory to store data in
    store_dir: PathBuf,
}

impl RethNode {
    /// Create a new Reth node.
    #[must_use]
    pub fn new(
        RethNodeOptions {
            discovery_port,
            http_port,
            metrics_port,
            network,
            rpc_port,
            store_dir,
        }: RethNodeOptions,
    ) -> Self {
        Self {
            process: None,
            discovery_port,
            http_port,
            metrics_port,
            network,
            rpc_port,
            store_dir,
        }
    }

    /// Start the Reth node.
    ///
    /// Returns a handle to the task that is running the node.
    pub async fn start(&mut self) -> Result<JoinHandle<()>, Error> {
        if self.process.is_some() {
            return Err(Error::AlreadyStarted);
        }

        info!("Starting Reth node...");

        self.prepare_config().await?;

        let app = RethApp {
            executable_path: "/apps/ethereum-reth/v1.3.8/reth".to_string(),
            discovery_port: self.discovery_port,
            http_port: self.http_port,
            metrics_port: self.metrics_port,
            network: self.network,
            rpc_port: self.rpc_port,
            store_dir: self.store_dir.clone(),
        };

        let (process, join_handle) = proven_isolation::spawn(app)
            .await
            .map_err(Error::Isolation)?;

        self.process = Some(process);

        Ok(join_handle)
    }

    /// Shuts down the Reth node.
    pub async fn shutdown(&mut self) -> Result<(), Error> {
        info!("Reth node shutting down...");

        if let Some(process) = self.process.take() {
            process.shutdown().await.map_err(Error::Isolation)?;
            info!("Reth node shutdown");
        } else {
            debug!("No running Reth node to shut down");
        }

        Ok(())
    }

    /// Returns the IP address of the Postgres server.
    #[must_use]
    pub fn ip_address(&self) -> IpAddr {
        self.process
            .as_ref()
            .map(|p| p.container_ip().unwrap())
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST))
    }

    /// Returns the JWT hex value for auth RPC.
    pub async fn jwt_hex(&self) -> Result<String, Error> {
        let jwt_hex = tokio::fs::read_to_string(self.store_dir.join("jwt.hex"))
            .await
            .map_err(|e| Error::Io("Failed to read jwt.hex", e))?;

        Ok(jwt_hex)
    }

    /// Returns the port of the Postgres server.
    #[must_use]
    pub fn rpc_port(&self) -> u16 {
        self.rpc_port
    }

    async fn prepare_config(&self) -> Result<(), Error> {
        // Create the data directory if it doesn't exist
        if !self.store_dir.exists() {
            fs::create_dir_all(&self.store_dir)
                .map_err(|e| Error::Io("Failed to create data directory", e))?;
        }
        Ok(())
    }
}
