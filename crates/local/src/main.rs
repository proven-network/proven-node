//! Binary to bootstrap other components locally.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod bootstrap;
mod error;
mod hosts;
mod net;
mod node;

use bootstrap::Bootstrap;
use error::Result;

use std::path::PathBuf;

use clap::Parser;
use tracing::info;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Bitcoin mainnet store directory
    #[arg(
        long,
        default_value = "/tmp/proven/bitcoin-mainnet",
        env = "PROVEN_BITCOIN_MAINNET_STORE_DIR"
    )]
    bitcoin_mainnet_store_dir: PathBuf,

    /// Bitcoin testnet store directory
    #[arg(
        long,
        default_value = "/tmp/proven/bitcoin-testnet",
        env = "PROVEN_BITCOIN_TESTNET_STORE_DIR"
    )]
    bitcoin_testnet_store_dir: PathBuf,

    /// Ethereum Holesky Consensus HTTP address
    #[arg(
        long,
        default_value = "5052",
        env = "PROVEN_ETHEREUM_HOLESKY_CONSENSUS_HTTP_PORT"
    )]
    ethereum_holesky_consensus_http_port: u16,

    /// Ethereum Holesky Consensus metrics address
    #[arg(
        long,
        default_value = "5054",
        env = "PROVEN_ETHEREUM_HOLESKY_CONSENSUS_METRICS_PORT"
    )]
    ethereum_holesky_consensus_metrics_port: u16,

    /// Ethereum Holesky Consensus P2P address
    #[arg(
        long,
        default_value = "9919",
        env = "PROVEN_ETHEREUM_HOLESKY_CONSENSUS_P2P_PORT"
    )]
    ethereum_holesky_consensus_p2p_port: u16,

    /// Ethereum Holesky Consensus store directory
    #[arg(
        long,
        default_value = "/tmp/proven/ethereum-holesky/lighthouse",
        env = "PROVEN_ETHEREUM_HOLESKY_CONSENSUS_STORE_DIR"
    )]
    ethereum_holesky_consensus_store_dir: PathBuf,

    /// Ethereum Holesky Execution discovery address
    #[arg(
        long,
        default_value = "30305",
        env = "PROVEN_ETHEREUM_HOLESKY_EXECUTION_DISCOVERY_PORT"
    )]
    ethereum_holesky_execution_discovery_port: u16,

    /// Ethereum Holesky Execution HTTP address
    #[arg(
        long,
        default_value = "8547",
        env = "PROVEN_ETHEREUM_HOLESKY_EXECUTION_HTTP_PORT"
    )]
    ethereum_holesky_execution_http_port: u16,

    /// Ethereum Holesky Execution metrics address
    #[arg(
        long,
        default_value = "9420",
        env = "PROVEN_ETHEREUM_HOLESKY_EXECUTION_METRICS_PORT"
    )]
    ethereum_holesky_execution_metrics_port: u16,

    /// Ethereum Holesky Execution RPC address
    #[arg(
        long,
        default_value = "8553",
        env = "PROVEN_ETHEREUM_HOLESKY_EXECUTION_RPC_PORT"
    )]
    ethereum_holesky_execution_rpc_port: u16,

    /// Ethereum Holesky store directory
    #[arg(
        long,
        default_value = "/tmp/proven/ethereum-holesky/reth",
        env = "PROVEN_ETHEREUM_HOLESKY_EXECUTION_STORE_DIR"
    )]
    ethereum_holesky_execution_store_dir: PathBuf,

    /// Ethereum Mainnet Consensus HTTP address
    #[arg(
        long,
        default_value = "5052",
        env = "PROVEN_ETHEREUM_MAINNET_CONSENSUS_HTTP_PORT"
    )]
    ethereum_mainnet_consensus_http_port: u16,

    /// Ethereum Mainnet Consensus metrics address
    #[arg(
        long,
        default_value = "5054",
        env = "PROVEN_ETHEREUM_MAINNET_CONSENSUS_METRICS_PORT"
    )]
    ethereum_mainnet_consensus_metrics_port: u16,

    /// Ethereum Mainnet Consensus P2P address
    #[arg(
        long,
        default_value = "9919",
        env = "PROVEN_ETHEREUM_MAINNET_CONSENSUS_P2P_PORT"
    )]
    ethereum_mainnet_consensus_p2p_port: u16,

    /// Ethereum Mainnet Consensus store directory
    #[arg(
        long,
        default_value = "/tmp/proven/ethereum-mainnet/lighthouse",
        env = "PROVEN_ETHEREUM_MAINNET_CONSENSUS_STORE_DIR"
    )]
    ethereum_mainnet_consensus_store_dir: PathBuf,

    /// Ethereum Mainnet Execution discovery address
    #[arg(
        long,
        default_value = "30303",
        env = "PROVEN_ETHEREUM_MAINNET_EXECUTION_DISCOVERY_PORT"
    )]
    ethereum_mainnet_execution_discovery_port: u16,

    /// Ethereum Mainnet Execution HTTP address
    #[arg(
        long,
        default_value = "8545",
        env = "PROVEN_ETHEREUM_MAINNET_EXECUTION_HTTP_PORT"
    )]
    ethereum_mainnet_execution_http_port: u16,

    /// Ethereum Mainnet Execution metrics address
    #[arg(
        long,
        default_value = "9418",
        env = "PROVEN_ETHEREUM_MAINNET_EXE  CUTION_METRICS_PORT"
    )]
    ethereum_mainnet_execution_metrics_port: u16,

    /// Ethereum Mainnet Execution RPC address
    #[arg(
        long,
        default_value = "8551",
        env = "PROVEN_ETHEREUM_MAINNET_EXECUTION_RPC_PORT"
    )]
    ethereum_mainnet_execution_rpc_port: u16,

    /// Ethereum Mainnet store directory
    #[arg(
        long,
        default_value = "/tmp/proven/ethereum-mainnet/reth",
        env = "PROVEN_ETHEREUM_MAINNET_EXECUTION_STORE_DIR"
    )]
    ethereum_mainnet_execution_store_dir: PathBuf,

    /// Ethereum Sepolia Consensus HTTP address
    #[arg(
        long,
        default_value = "5052",
        env = "PROVEN_ETHEREUM_SEPOLIA_CONSENSUS_HTTP_PORT"
    )]
    ethereum_sepolia_consensus_http_port: u16,

    /// Ethereum Sepolia Consensus metrics address
    #[arg(
        long,
        default_value = "5054",
        env = "PROVEN_ETHEREUM_SEPOLIA_CONSENSUS_METRICS_PORT"
    )]
    ethereum_sepolia_consensus_metrics_port: u16,

    /// Ethereum Sepolia Consensus P2P address
    #[arg(
        long,
        default_value = "9919",
        env = "PROVEN_ETHEREUM_SEPOLIA_CONSENSUS_P2P_PORT"
    )]
    ethereum_sepolia_consensus_p2p_port: u16,

    /// Ethereum Sepolia Consensus store directory
    #[arg(
        long,
        default_value = "/tmp/proven/ethereum-sepolia/lighthouse",
        env = "PROVEN_ETHEREUM_SEPOLIA_CONSENSUS_STORE_DIR"
    )]
    ethereum_sepolia_consensus_store_dir: PathBuf,

    /// Ethereum Sepolia Execution discovery address
    #[arg(
        long,
        default_value = "30304",
        env = "PROVEN_ETHEREUM_SEPOLIA_EXECUTION_DISCOVERY_PORT"
    )]
    ethereum_sepolia_execution_discovery_port: u16,

    /// Ethereum Sepolia Execution HTTP address
    #[arg(
        long,
        default_value = "8546",
        env = "PROVEN_ETHEREUM_SEPOLIA_EXECUTION_HTTP_PORT"
    )]
    ethereum_sepolia_execution_http_port: u16,

    /// Ethereum Sepolia Execution metrics address
    #[arg(
        long,
        default_value = "9419",
        env = "PROVEN_ETHEREUM_SEPOLIA_EXECUTION_METRICS_PORT"
    )]
    ethereum_sepolia_execution_metrics_port: u16,

    /// Ethereum Sepolia Execution RPC address
    #[arg(
        long,
        default_value = "8552",
        env = "PROVEN_ETHEREUM_SEPOLIA_EXECUTION_RPC_PORT"
    )]
    ethereum_sepolia_execution_rpc_port: u16,

    /// Ethereum Sepolia store directory
    #[arg(
        long,
        default_value = "/tmp/proven/ethereum-sepolia/reth",
        env = "PROVEN_ETHEREUM_SEPOLIA_EXECUTION_STORE_DIR"
    )]
    ethereum_sepolia_execution_store_dir: PathBuf,

    /// Proven HTTP port
    #[arg(long, default_value_t = 3200, env = "PROVEN_PORT")]
    port: u16,

    /// NATS port
    #[arg(long, default_value_t = 4222, env = "PROVEN_NATS_CLIENT_PORT")]
    nats_client_port: u16,

    /// NATS cluster port
    #[arg(long, default_value_t = 6222, env = "PROVEN_NATS_CLUSTER_PORT")]
    nats_cluster_port: u16,

    /// NATS store directory
    #[arg(
        long,
        default_value = "/tmp/proven/nats",
        env = "PROVEN_NATS_STORE_DIR"
    )]
    nats_store_dir: PathBuf,

    /// Private key provided directly as an environment variable
    #[arg(long, env = "PROVEN_NODE_KEY", required = true)]
    node_key: String,

    /// Postgres binary directory path
    #[arg(
        long,
        default_value = "/usr/local/pgsql/bin",
        env = "POSTGRES_BIN_PATH"
    )]
    postgres_bin_path: String,

    /// Postgres port
    #[arg(long, default_value_t = 5432, env = "PROVEN_POSTGRES_PORT")]
    postgres_port: u16,

    /// Postgres store directory
    #[arg(
        long,
        default_value = "/tmp/proven/postgres",
        env = "POSTGRES_STORE_DIR"
    )]
    postgres_store_dir: PathBuf,

    /// Radix Mainnet HTTP port
    #[arg(long, default_value_t = 3333, env = "PROVEN_RADIX_MAINNET_HTTP_PORT")]
    radix_mainnet_http_port: u16,

    /// Radix Mainnet port
    #[arg(long, default_value_t = 30000, env = "PROVEN_RADIX_MAINNET_P2P_PORT")]
    radix_mainnet_p2p_port: u16,

    /// Radix Mainnet store directory
    #[arg(
        long,
        default_value = "/tmp/proven/radix-node-mainnet",
        env = "PROVEN_RADIX_MAINNET_STORE_DIR"
    )]
    radix_mainnet_store_dir: PathBuf,

    /// Radix Stokenet HTTP port
    #[arg(long, default_value_t = 3343, env = "PROVEN_RADIX_STOKENET_HTTP_PORT")]
    radix_stokenet_http_port: u16,

    /// Radix Stokenet port
    #[arg(long, default_value_t = 30001, env = "PROVEN_RADIX_STOKENET_P2P_PORT")]
    radix_stokenet_p2p_port: u16,

    /// Radix Stokenet store directory
    #[arg(
        long,
        default_value = "/tmp/proven/radix-node-stokenet",
        env = "PROVEN_RADIX_STOKENET_STORE_DIR"
    )]
    radix_stokenet_store_dir: PathBuf,

    /// Skip vacuuming the database
    #[arg(long, env = "PROVEN_SKIP_VACUUM")]
    skip_vacuum: bool,

    /// Use testnet
    #[arg(long, env = "PROVEN_TESTNET")]
    testnet: bool,

    /// Path to the topology file
    #[arg(long, env = "PROVEN_TOPOLOGY_FILE")]
    topology_file: Option<PathBuf>,
}

#[tokio::main(worker_threads = 8)]
async fn main() -> Result<()> {
    // Initialize tracing for better logging
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let bootstrap = Bootstrap::new(args).await?;
    let node = bootstrap.initialize().await?;

    // Wait for either SIGINT (Ctrl-C) or SIGTERM
    if cfg!(unix) {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler failed");
        let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler failed");

        tokio::select! {
            _ = sigterm.recv() => info!("Received SIGTERM"),
            _ = sigint.recv() => info!("Received SIGINT"),
        }
    } else {
        // Fall back to just ctrl-c on non-unix platforms
        let _ = tokio::signal::ctrl_c().await;
    }

    info!("Shutting down");

    let () = node.shutdown().await;

    info!("Shutdown complete");

    Ok(())
}
