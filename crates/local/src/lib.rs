//! Library for bootstrapping proven components locally.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]
#![allow(clippy::large_futures)] // TODO: Potential refactor

mod bootstrap;
mod error;
mod hosts;
mod net;

pub use bootstrap::Bootstrap;
use ed25519_dalek::SigningKey;
pub use error::Error;
use proven_governance::Governance;

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::info;
use url::Url;

/// Status of a node
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeStatus {
    /// Node is not started yet
    NotStarted,
    /// Node is in the process of starting
    Starting,
    /// Node is running normally  
    Running,
    /// Node is in the process of stopping
    Stopping,
    /// Node has stopped cleanly
    Stopped,
    /// Node failed during startup or runtime
    Failed(String),
}

impl std::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotStarted => write!(f, "Not Started"),
            Self::Starting => write!(f, "Starting"),
            Self::Running => write!(f, "Running"),
            Self::Stopping => write!(f, "Stopping"),
            Self::Stopped => write!(f, "Stopped"),
            Self::Failed(err) => write!(f, "Failed: {err}"),
        }
    }
}

/// Configuration for a node instance
#[derive(Clone, Debug)]
pub struct NodeConfig<G: Governance> {
    /// Whether to allow a single node to be started
    pub allow_single_node: bool,

    /// Bitcoin mainnet fallback RPC endpoint
    pub bitcoin_mainnet_fallback_rpc_endpoint: Url,

    /// Bitcoin mainnet proxy port
    pub bitcoin_mainnet_proxy_port: u16,

    /// Bitcoin mainnet store directory
    pub bitcoin_mainnet_store_dir: PathBuf,

    /// Bitcoin testnet fallback RPC endpoint  
    pub bitcoin_testnet_fallback_rpc_endpoint: Url,

    /// Bitcoin testnet proxy port
    pub bitcoin_testnet_proxy_port: u16,

    /// Bitcoin testnet store directory
    pub bitcoin_testnet_store_dir: PathBuf,

    /// Ethereum Holesky Consensus HTTP address
    pub ethereum_holesky_consensus_http_port: u16,

    /// Ethereum Holesky Consensus metrics address
    pub ethereum_holesky_consensus_metrics_port: u16,

    /// Ethereum Holesky Consensus P2P address
    pub ethereum_holesky_consensus_p2p_port: u16,

    /// Ethereum Holesky Consensus store directory
    pub ethereum_holesky_consensus_store_dir: PathBuf,

    /// Ethereum Holesky Execution discovery address
    pub ethereum_holesky_execution_discovery_port: u16,

    /// Ethereum Holesky Execution HTTP address
    pub ethereum_holesky_execution_http_port: u16,

    /// Ethereum Holesky Execution metrics address
    pub ethereum_holesky_execution_metrics_port: u16,

    /// Ethereum Holesky Execution RPC address
    pub ethereum_holesky_execution_rpc_port: u16,

    /// Ethereum Holesky store directory
    pub ethereum_holesky_execution_store_dir: PathBuf,

    /// Ethereum Holesky fallback RPC endpoint
    pub ethereum_holesky_fallback_rpc_endpoint: Url,

    /// Ethereum Mainnet Consensus HTTP address
    pub ethereum_mainnet_consensus_http_port: u16,

    /// Ethereum Mainnet Consensus metrics address
    pub ethereum_mainnet_consensus_metrics_port: u16,

    /// Ethereum Mainnet Consensus P2P address
    pub ethereum_mainnet_consensus_p2p_port: u16,

    /// Ethereum Mainnet Consensus store directory
    pub ethereum_mainnet_consensus_store_dir: PathBuf,

    /// Ethereum Mainnet Execution discovery address
    pub ethereum_mainnet_execution_discovery_port: u16,

    /// Ethereum Mainnet Execution HTTP address
    pub ethereum_mainnet_execution_http_port: u16,

    /// Ethereum Mainnet Execution metrics address
    pub ethereum_mainnet_execution_metrics_port: u16,

    /// Ethereum Mainnet Execution RPC address
    pub ethereum_mainnet_execution_rpc_port: u16,

    /// Ethereum Mainnet store directory
    pub ethereum_mainnet_execution_store_dir: PathBuf,

    /// Ethereum Mainnet fallback RPC endpoint
    pub ethereum_mainnet_fallback_rpc_endpoint: Url,

    /// Ethereum Sepolia Consensus HTTP address
    pub ethereum_sepolia_consensus_http_port: u16,

    /// Ethereum Sepolia Consensus metrics address
    pub ethereum_sepolia_consensus_metrics_port: u16,

    /// Ethereum Sepolia Consensus P2P address
    pub ethereum_sepolia_consensus_p2p_port: u16,

    /// Ethereum Sepolia Consensus store directory
    pub ethereum_sepolia_consensus_store_dir: PathBuf,

    /// Ethereum Sepolia Execution discovery address
    pub ethereum_sepolia_execution_discovery_port: u16,

    /// Ethereum Sepolia Execution HTTP address
    pub ethereum_sepolia_execution_http_port: u16,

    /// Ethereum Sepolia Execution metrics address
    pub ethereum_sepolia_execution_metrics_port: u16,

    /// Ethereum Sepolia Execution RPC address
    pub ethereum_sepolia_execution_rpc_port: u16,

    /// Ethereum Sepolia store directory
    pub ethereum_sepolia_execution_store_dir: PathBuf,

    /// Ethereum Sepolia fallback RPC endpoint
    pub ethereum_sepolia_fallback_rpc_endpoint: Url,

    /// Proven governance
    pub governance: G,

    /// Proven HTTP port
    pub port: u16,

    /// NATS CLI binary directory path
    pub nats_cli_bin_dir: Option<PathBuf>,

    /// NATS port
    pub nats_client_port: u16,

    /// NATS cluster port
    pub nats_cluster_port: u16,

    /// NATS config directory
    pub nats_config_dir: PathBuf,

    /// NATS HTTP port
    pub nats_http_port: u16,

    /// NATS server binary directory path
    pub nats_server_bin_dir: Option<PathBuf>,

    /// NATS store directory
    pub nats_store_dir: PathBuf,

    /// Path to the network config file
    pub network_config_path: Option<PathBuf>,

    /// Private key provided directly as an environment variable
    pub node_key: SigningKey,

    /// Postgres binary directory path
    pub postgres_bin_path: PathBuf,

    /// Postgres port
    pub postgres_port: u16,

    /// Skip vacuuming the database
    pub postgres_skip_vacuum: bool,

    /// Postgres store directory
    pub postgres_store_dir: PathBuf,

    /// Radix Mainnet fallback RPC endpoint
    pub radix_mainnet_fallback_rpc_endpoint: Url,

    /// Radix Mainnet HTTP port
    pub radix_mainnet_http_port: u16,

    /// Radix Mainnet port
    pub radix_mainnet_p2p_port: u16,

    /// Radix Mainnet store directory
    pub radix_mainnet_store_dir: PathBuf,

    /// Radix Stokenet fallback RPC endpoint
    pub radix_stokenet_fallback_rpc_endpoint: Url,

    /// Radix Stokenet HTTP port
    pub radix_stokenet_http_port: u16,

    /// Radix Stokenet port
    pub radix_stokenet_p2p_port: u16,

    /// Radix Stokenet store directory
    pub radix_stokenet_store_dir: PathBuf,
}

/// A managed node instance that can be started, stopped, and queried
pub struct Node<G: Governance> {
    config: NodeConfig<G>,
    status: Arc<RwLock<NodeStatus>>,
    shutdown_token: Option<CancellationToken>,
    task_handle: Option<tokio::task::JoinHandle<Result<(), Error>>>,
}

impl<G: Governance> Node<G> {
    /// Create a new node with the given configuration
    pub fn new(config: NodeConfig<G>) -> Self {
        Self {
            config,
            status: Arc::new(RwLock::new(NodeStatus::NotStarted)),
            shutdown_token: None,
            task_handle: None,
        }
    }

    /// Get the current status of the node
    pub async fn status(&self) -> NodeStatus {
        self.status.read().await.clone()
    }

    /// Check if the node is currently running
    pub async fn is_running(&self) -> bool {
        matches!(*self.status.read().await, NodeStatus::Running)
    }

    /// Start the node
    ///
    /// # Errors
    ///
    /// Returns an error if the node fails to start or is already started
    pub async fn start(&mut self) -> Result<(), Error> {
        let current_status = self.status.read().await.clone();

        match current_status {
            NodeStatus::NotStarted | NodeStatus::Stopped | NodeStatus::Failed(_) => {
                // OK to start
            }
            _ => {
                return Err(Error::Io(format!(
                    "Cannot start node that is in state: {current_status}"
                )));
            }
        }

        // Set status to starting
        *self.status.write().await = NodeStatus::Starting;

        // Create shutdown token
        let shutdown_token = CancellationToken::new();
        self.shutdown_token = Some(shutdown_token.clone());

        // Clone status handle for the task
        let status = self.status.clone();
        let config = self.config.clone();

        // Start the node in a background task
        let task_handle = tokio::spawn(async move {
            let result = run_node_internal(config, shutdown_token, status.clone()).await;

            // Update status based on result
            match &result {
                Ok(()) => {
                    *status.write().await = NodeStatus::Stopped;
                }
                Err(e) => {
                    *status.write().await = NodeStatus::Failed(e.to_string());
                }
            }

            result
        });

        self.task_handle = Some(task_handle);

        Ok(())
    }

    /// Stop the node
    ///
    /// # Errors
    ///
    /// Returns an error if the node fails to stop cleanly
    pub async fn stop(&mut self) -> Result<(), Error> {
        let current_status = self.status.read().await.clone();

        match current_status {
            NodeStatus::Running | NodeStatus::Starting => {
                // OK to stop
            }
            _ => {
                return Err(Error::Io(format!(
                    "Cannot stop node that is in state: {current_status}"
                )));
            }
        }

        // Set status to stopping
        *self.status.write().await = NodeStatus::Stopping;

        // Cancel the shutdown token if it exists
        if let Some(shutdown_token) = &self.shutdown_token {
            shutdown_token.cancel();
        }

        // Wait for the task to complete
        if let Some(task_handle) = self.task_handle.take() {
            match task_handle.await {
                Ok(Ok(())) => {
                    info!("Node stopped successfully");
                }
                Ok(Err(e)) => {
                    info!("Node stopped with error: {e}");
                    *self.status.write().await = NodeStatus::Failed(e.to_string());
                    return Err(e);
                }
                Err(join_error) => {
                    let error_msg = format!("Node task panicked: {join_error}");
                    info!("{error_msg}");
                    *self.status.write().await = NodeStatus::Failed(error_msg.clone());
                    return Err(Error::Io(error_msg));
                }
            }
        }

        self.shutdown_token = None;
        Ok(())
    }

    /// Get the node's configuration
    pub const fn config(&self) -> &NodeConfig<G> {
        &self.config
    }
}

/// Internal function to run a node
async fn run_node_internal<G: Governance>(
    config: NodeConfig<G>,
    shutdown_token: CancellationToken,
    status: Arc<RwLock<NodeStatus>>,
) -> Result<(), Error> {
    // Start bootstrap with shared shutdown token
    let bootstrap = Bootstrap::new(config).await?;

    let initialization_result = tokio::select! {
        result = bootstrap.initialize(shutdown_token.clone()) => {
            result
        }
        () = shutdown_token.cancelled() => {
            info!("Shutdown requested during initialization");
            return Ok(());
        }
    };

    match initialization_result {
        Ok((_node, task_tracker)) => {
            info!("Bootstrap completed successfully");

            // Update status to running
            *status.write().await = NodeStatus::Running;

            // Wait for shutdown
            shutdown_token.cancelled().await;

            // Update status to stopping
            *status.write().await = NodeStatus::Stopping;

            task_tracker.wait().await;

            info!("Shutdown complete");
        }

        Err(e) => {
            info!("Bootstrap failed: {:?}", e);
            return Err(e);
        }
    }

    Ok(())
}

/// Run a node with the given configuration (legacy function, kept for backwards compatibility)
///
/// This function starts the bootstrap process and waits for it to complete.
/// It also sets up a shutdown token to allow the node to be shutdown.
///
/// # Errors
///
/// This function returns an error if the bootstrap process fails.
pub async fn run_node<G: Governance>(
    config: NodeConfig<G>,
    shutdown_token: CancellationToken,
) -> Result<(), Error> {
    let status = Arc::new(RwLock::new(NodeStatus::Starting));
    run_node_internal(config, shutdown_token, status).await
}
