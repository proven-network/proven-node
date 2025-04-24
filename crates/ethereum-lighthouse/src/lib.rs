//! Configures and runs the Lighthouse Ethereum consensus client.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;

pub use error::Error;
use regex::Regex;

use std::error::Error as StdError;
use std::fs;
use std::path::PathBuf;
use std::sync::RwLock;

use async_trait::async_trait;
use once_cell::sync::Lazy;
use proven_isolation::{IsolatedApplication, IsolatedProcess, ReadyCheckInfo, VolumeMount};
use reqwest::Client;
use serde_json;
use strip_ansi_escapes::strip_str;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace, warn};

// Rust log regexp
static LOG_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"[A-Z][a-z]{2}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\.\d{3}\s+(\w+)\s+(.*)").unwrap()
});

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

/// Options for configuring a `LighthouseNode`.
pub struct LighthouseNodeOptions {
    /// The execution client RPC IP address.
    pub execution_rpc_ip_address: String,

    /// The execution client RPC JWT hex.
    pub execution_rpc_jwt_hex: String,

    /// The execution client RPC port.
    pub execution_rpc_port: u16,

    /// The host IP address.
    pub host_ip: String,

    /// The HTTP API port.
    pub http_port: u16,

    /// The metrics port.
    pub metrics_port: u16,

    /// The Ethereum network to connect to.
    pub network: EthereumNetwork,

    /// The P2P networking port.
    pub p2p_port: u16,

    /// The directory to store data in.
    pub store_dir: PathBuf,
}

/// Lighthouse application implementing the IsolatedApplication trait
struct LighthouseApp {
    /// The path to the lighthouse executable
    executable_path: String,

    /// The execution client RPC IP address
    execution_rpc_ip_address: String,

    /// The execution client RPC JWT hex
    execution_rpc_jwt_hex: String,

    /// The execution client RPC port
    execution_rpc_port: u16,

    /// The host IP address
    host_ip: String,

    /// The HTTP API port
    http_port: u16,

    /// The last seen log level
    last_log_level: RwLock<String>,

    /// The metrics port
    metrics_port: u16,

    /// The Ethereum network type
    network: EthereumNetwork,

    /// The P2P networking port
    p2p_port: u16,

    /// The directory to store data in
    store_dir: PathBuf,
}

#[async_trait]
impl IsolatedApplication for LighthouseApp {
    fn args(&self) -> Vec<String> {
        let mut args = vec!["beacon_node".to_string()];

        // Add data directory
        args.push("--datadir=/data".to_string());

        // Add network-specific args
        match self.network {
            EthereumNetwork::Mainnet => {}
            EthereumNetwork::Sepolia => {
                args.push("--network=sepolia".to_string());
            }
            EthereumNetwork::Holesky => {
                args.push("--network=holesky".to_string());
            }
        }

        // Add execution endpoint
        args.push("--execution-endpoint".to_string());
        args.push(format!(
            "http://{}:{}",
            self.execution_rpc_ip_address, self.execution_rpc_port
        ));

        // Add JWT secret
        args.push("--execution-jwt-secret-key".to_string());
        args.push(self.execution_rpc_jwt_hex.clone());

        // Add P2P networking args
        args.push("--listen-address".to_string());
        args.push("0.0.0.0".to_string());
        args.push("--port".to_string());
        args.push(self.p2p_port.to_string());

        // Add HTTP API args
        args.push("--http".to_string());
        args.push("--http-address".to_string());
        args.push("0.0.0.0".to_string());
        args.push("--http-port".to_string());
        args.push(self.http_port.to_string());

        // Add metrics args
        args.push("--metrics".to_string());
        args.push("--metrics-address".to_string());
        args.push("0.0.0.0".to_string());
        args.push("--metrics-port".to_string());
        args.push(self.metrics_port.to_string());

        // Add checkpoint sync URL
        args.push("--checkpoint-sync-url".to_string());
        args.push(get_checkpoint_sync_url(self.network));

        // Add ENR IP address
        args.push("--enr-address".to_string());
        args.push(self.host_ip.clone());

        // Disable UPNP
        args.push("--disable-upnp".to_string());

        args
    }

    fn executable(&self) -> &str {
        &self.executable_path
    }

    fn handle_stderr(&self, line: &str) -> () {
        self.handle_stdout(line);
    }

    fn handle_stdout(&self, line: &str) {
        if let Some(caps) = LOG_REGEX.captures(&strip_str(&line)) {
            let label = caps.get(1).map_or("UNKNW", |m| m.as_str());
            let message = caps.get(2).map_or(line, |m| m.as_str());
            *self.last_log_level.write().unwrap() = label.to_string();
            match label {
                "DEBU" => debug!(target: "lighthouse", "{}", message),
                "ERRO" => error!(target: "lighthouse", "{}", message),
                "INFO" => info!(target: "lighthouse", "{}", message),
                "TRAC" => trace!(target: "lighthouse", "{}", message),
                "WARN" => warn!(target: "lighthouse", "{}", message),
                _ => error!(target: "lighthouse", "{}", line),
            }
        } else {
            // Use the last log level for continuation lines
            match self.last_log_level.read().unwrap().as_str() {
                "DEBU" => debug!(target: "lighthouse", "{}", line),
                "ERRO" => error!(target: "lighthouse", "{}", line),
                "INFO" => info!(target: "lighthouse", "{}", line),
                "TRAC" => trace!(target: "lighthouse", "{}", line),
                "WARN" => warn!(target: "lighthouse", "{}", line),
                _ => error!(target: "lighthouse", "{}", line),
            }
        }
    }

    fn name(&self) -> &str {
        "lighthouse"
    }

    fn memory_limit_mb(&self) -> usize {
        // Lighthouse can be memory intensive, allocate 16GB by default
        16 * 1024
    }

    async fn is_ready_check(&self, info: ReadyCheckInfo) -> Result<bool, Box<dyn StdError>> {
        let http_url = format!("http://{}:{}", info.ip_address, self.http_port);

        let client = Client::new();
        match client
            .get(format!("{}/lighthouse/syncing", http_url))
            .send()
            .await
        {
            Ok(response) => {
                if response.status() == 200 {
                    match response.json::<serde_json::Value>().await {
                        Ok(json) => {
                            if let Some(data) = json.get("data") {
                                if data.is_string() {
                                    match data.as_str() {
                                        Some("Synced") => return Ok(true),
                                        Some("Stalled") => return Ok(false),
                                        _ => return Ok(false),
                                    }
                                } else if data.is_object() && data.get("SyncingFinalized").is_some()
                                {
                                    return Ok(false);
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
        vec![self.p2p_port]
    }

    fn udp_port_forwards(&self) -> Vec<u16> {
        vec![self.p2p_port]
    }

    fn volume_mounts(&self) -> Vec<VolumeMount> {
        vec![
            VolumeMount::new(self.store_dir.to_str().unwrap(), "/data"),
            VolumeMount::new(
                "/apps/ethereum-lighthouse/v7.0.0",
                "/apps/ethereum-lighthouse/v7.0.0",
            ),
        ]
    }
}

/// Runs a Lighthouse consensus client.
pub struct LighthouseNode {
    /// The isolated process running Lighthouse
    process: Option<IsolatedProcess>,

    /// The execution client RPC IP address
    execution_rpc_ip_address: String,

    /// The execution client RPC JWT hex
    execution_rpc_jwt_hex: String,

    /// The execution client RPC port
    execution_rpc_port: u16,

    /// The host IP address
    host_ip: String,

    /// The HTTP API port
    http_port: u16,

    /// The metrics port
    metrics_port: u16,

    /// The Ethereum network type
    network: EthereumNetwork,

    /// The P2P networking port
    p2p_port: u16,

    /// The directory to store data in
    store_dir: PathBuf,
}

impl LighthouseNode {
    /// Creates a new instance of `LighthouseNode`.
    #[must_use]
    pub fn new(
        LighthouseNodeOptions {
            execution_rpc_ip_address,
            execution_rpc_jwt_hex,
            execution_rpc_port,
            host_ip,
            http_port,
            metrics_port,
            network,
            p2p_port,
            store_dir,
        }: LighthouseNodeOptions,
    ) -> Self {
        Self {
            process: None,
            execution_rpc_ip_address,
            execution_rpc_jwt_hex,
            execution_rpc_port,
            host_ip,
            http_port,
            metrics_port,
            network,
            p2p_port,
            store_dir,
        }
    }

    /// Starts the Lighthouse consensus client.
    ///
    /// # Errors
    /// Returns an error if:
    /// - Data directories cannot be created
    /// - The Lighthouse process fails to start
    /// - The Lighthouse HTTP endpoint fails to become available
    pub async fn start(&mut self) -> Result<JoinHandle<()>, Error> {
        if self.process.is_some() {
            return Err(Error::AlreadyStarted);
        }

        info!("Starting Lighthouse node...");

        self.prepare_config().await?;

        let app = LighthouseApp {
            executable_path: "/apps/ethereum-lighthouse/v7.0.0/lighthouse".to_string(),
            execution_rpc_ip_address: self.execution_rpc_ip_address.clone(),
            execution_rpc_jwt_hex: self.execution_rpc_jwt_hex.clone(),
            execution_rpc_port: self.execution_rpc_port,
            host_ip: self.host_ip.clone(),
            http_port: self.http_port,
            last_log_level: RwLock::new("INFO".to_string()),
            metrics_port: self.metrics_port,
            network: self.network,
            p2p_port: self.p2p_port,
            store_dir: self.store_dir.clone(),
        };

        let (process, join_handle) = proven_isolation::spawn(app)
            .await
            .map_err(Error::Isolation)?;

        self.process = Some(process);

        Ok(join_handle)
    }

    /// Shuts down the Lighthouse node.
    pub async fn shutdown(&mut self) -> Result<(), Error> {
        info!("Lighthouse node shutting down...");

        if let Some(process) = self.process.take() {
            process.shutdown().await.map_err(Error::Isolation)?;
            info!("Lighthouse node shutdown");
        } else {
            debug!("No running Lighthouse node to shut down");
        }

        Ok(())
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

// Returns the appropriate checkpoint sync URL based on the network
fn get_checkpoint_sync_url(network: EthereumNetwork) -> String {
    match network {
        EthereumNetwork::Mainnet => "https://beaconstate-mainnet.chainsafe.io".to_string(),
        EthereumNetwork::Sepolia => "https://beaconstate-sepolia.chainsafe.io".to_string(),
        EthereumNetwork::Holesky => "https://beaconstate-holesky.chainsafe.io".to_string(),
    }
}
