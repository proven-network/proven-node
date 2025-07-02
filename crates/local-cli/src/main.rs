//! CLI binary to bootstrap proven components locally.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]
#![allow(clippy::large_futures)] // TODO: Potential refactor

use std::path::PathBuf;

use clap::Parser;
use ed25519_dalek::SigningKey;
use proven_attestation::Attestor;
use proven_attestation_mock::MockAttestor;
use proven_governance::Version;
use proven_governance_mock::MockGovernance;
use proven_local::{NodeConfig, run_node};
use tokio_util::sync::CancellationToken;
use tracing::info;
use url::Url;

/// CLI-specific error type
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Attestation error
    #[error("attestation error: {0}")]
    Attestation(String),

    /// Governance mock error
    #[error("governance error: {0}")]
    Governance(#[from] proven_governance_mock::Error),

    /// Local library error
    #[error(transparent)]
    Local(#[from] proven_local::Error),

    /// Network configuration error
    #[error("network config error: {0}")]
    NetworkConfig(String),

    /// Private key parsing error
    #[error("private key error: {0}")]
    PrivateKey(String),
}

#[derive(Clone, Debug, Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Bitcoin mainnet fallback RPC endpoint
    #[arg(
        long,
        default_value = "https://bitcoin-rpc.publicnode.com",
        env = "PROVEN_BITCOIN_MAINNET_FALLBACK_RPC_ENDPOINT"
    )]
    bitcoin_mainnet_fallback_rpc_endpoint: Url,

    /// Bitcoin mainnet proxy port
    #[arg(
        long,
        default_value_t = 11000,
        env = "PROVEN_BITCOIN_MAINNET_PROXY_PORT"
    )]
    bitcoin_mainnet_proxy_port: u16,

    /// Bitcoin mainnet RPC port
    #[arg(long, default_value_t = 8332, env = "PROVEN_BITCOIN_MAINNET_RPC_PORT")]
    bitcoin_mainnet_rpc_port: u16,

    /// Bitcoin mainnet store directory
    #[arg(
        long,
        default_value = "/tmp/proven/bitcoin-mainnet",
        env = "PROVEN_BITCOIN_MAINNET_STORE_DIR"
    )]
    bitcoin_mainnet_store_dir: PathBuf,

    /// Bitcoin testnet fallback RPC endpoint
    #[arg(
        long,
        default_value = "https://bitcoin-testnet-rpc.publicnode.com",
        env = "PROVEN_BITCOIN_TESTNET_FALLBACK_RPC_ENDPOINT"
    )]
    bitcoin_testnet_fallback_rpc_endpoint: Url,

    /// Bitcoin testnet proxy port
    #[arg(
        long,
        default_value_t = 11001,
        env = "PROVEN_BITCOIN_TESTNET_PROXY_PORT"
    )]
    bitcoin_testnet_proxy_port: u16,

    /// Bitcoin testnet RPC port
    #[arg(long, default_value_t = 18332, env = "PROVEN_BITCOIN_TESTNET_RPC_PORT")]
    bitcoin_testnet_rpc_port: u16,

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

    /// Ethereum Holesky fallback RPC endpoint
    #[arg(
        long,
        default_value = "https://ethereum-holesky-rpc.publicnode.com",
        env = "PROVEN_ETHEREUM_HOLESKY_FALLBACK_RPC_ENDPOINT"
    )]
    ethereum_holesky_fallback_rpc_endpoint: Url,

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
        env = "PROVEN_ETHEREUM_MAINNET_EXECUTION_METRICS_PORT"
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

    /// Ethereum Mainnet fallback RPC endpoint
    #[arg(
        long,
        default_value = "https://ethereum-rpc.publicnode.com",
        env = "PROVEN_ETHEREUM_MAINNET_FALLBACK_RPC_ENDPOINT"
    )]
    ethereum_mainnet_fallback_rpc_endpoint: Url,

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

    /// Ethereum Sepolia fallback RPC endpoint
    #[arg(
        long,
        default_value = "https://ethereum-sepolia-rpc.publicnode.com",
        env = "PROVEN_ETHEREUM_SEPOLIA_FALLBACK_RPC_ENDPOINT"
    )]
    ethereum_sepolia_fallback_rpc_endpoint: Url,

    /// Proven HTTP port
    #[arg(long, default_value_t = 3200, env = "PROVEN_PORT")]
    port: u16,

    /// NATS CLI binary directory path
    #[arg(long, env = "PROVEN_NATS_CLI_BIN_DIR")]
    nats_cli_bin_dir: Option<PathBuf>,

    /// NATS port
    #[arg(long, default_value_t = 4222, env = "PROVEN_NATS_CLIENT_PORT")]
    nats_client_port: u16,

    /// NATS cluster port
    #[arg(long, default_value_t = 6222, env = "PROVEN_NATS_CLUSTER_PORT")]
    nats_cluster_port: u16,

    /// NATS config directory
    #[arg(
        long,
        default_value = "/tmp/proven/nats-config",
        env = "PROVEN_NATS_CONFIG_DIR"
    )]
    nats_config_dir: PathBuf,

    /// NATS HTTP port
    #[arg(long, default_value_t = 8222, env = "PROVEN_NATS_HTTP_PORT")]
    nats_http_port: u16,

    /// NATS server binary directory path
    #[arg(long, env = "PROVEN_NATS_SERVER_BIN_DIR")]
    nats_server_bin_dir: Option<PathBuf>,

    /// NATS store directory
    #[arg(
        long,
        default_value = "/tmp/proven/nats",
        env = "PROVEN_NATS_STORE_DIR"
    )]
    nats_store_dir: PathBuf,

    /// Path to the network config file
    #[arg(long, env = "PROVEN_NETWORK_CONFIG_PATH")]
    network_config_path: Option<PathBuf>,

    /// Private key provided directly as an environment variable
    #[arg(long, env = "PROVEN_NODE_KEY", required = true)]
    node_key: String,

    /// P2P port
    #[arg(long, default_value_t = 40000, env = "PROVEN_P2P_PORT")]
    p2p_port: u16,

    /// Postgres binary directory path
    #[arg(
        long,
        default_value = "/usr/local/pgsql/bin",
        env = "POSTGRES_BIN_PATH"
    )]
    postgres_bin_path: PathBuf,

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

    /// Radix Mainnet fallback RPC endpoint
    #[arg(
        long,
        default_value = "https://mainnet.radixdlt.com",
        env = "PROVEN_RADIX_MAINNET_FALLBACK_RPC_ENDPOINT"
    )]
    radix_mainnet_fallback_rpc_endpoint: Url,

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

    /// Radix Stokenet fallback RPC endpoint
    #[arg(
        long,
        default_value = "https://stokenet.radixdlt.com",
        env = "PROVEN_RADIX_STOKENET_FALLBACK_RPC_ENDPOINT"
    )]
    radix_stokenet_fallback_rpc_endpoint: Url,

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
}

/// Create a `NodeConfig` from Args, handling async operations
async fn create_node_config(args: Args) -> Result<NodeConfig<MockGovernance>, Error> {
    // Parse the private key and calculate public key
    let private_key_bytes = hex::decode(args.node_key.trim())
        .map_err(|e| Error::PrivateKey(format!("Failed to decode private key as hex: {e}")))?;

    // We need exactly 32 bytes for ed25519 private key
    let private_key = SigningKey::try_from(private_key_bytes.as_slice()).map_err(|_| {
        Error::PrivateKey("Failed to create SigningKey: invalid key length".to_string())
    })?;

    // Create governance based on network config
    let mut allow_single_node = false;
    let governance = if let Some(ref network_config_path) = args.network_config_path {
        info!(
            "using replication factor 3 with network config from file: {}",
            network_config_path.display()
        );

        MockGovernance::from_network_config_file(network_config_path)
            .map_err(|e| Error::NetworkConfig(format!("Failed to load network config: {e}")))?
    } else {
        info!("using replication factor 1 as no network config file provided");
        allow_single_node = true;

        let pcrs = MockAttestor::new()
            .pcrs()
            .await
            .map_err(|e| Error::Attestation(format!("Failed to get PCRs: {e}")))?;
        let version = Version::from_pcrs(pcrs);

        MockGovernance::for_single_node(
            format!("http://localhost:{}", args.port),
            &private_key,
            version,
        )
    };

    Ok(NodeConfig {
        allow_single_node,
        bitcoin_mainnet_fallback_rpc_endpoint: args.bitcoin_mainnet_fallback_rpc_endpoint,
        bitcoin_mainnet_proxy_port: args.bitcoin_mainnet_proxy_port,
        bitcoin_mainnet_rpc_port: args.bitcoin_mainnet_rpc_port,
        bitcoin_mainnet_store_dir: args.bitcoin_mainnet_store_dir,
        bitcoin_testnet_fallback_rpc_endpoint: args.bitcoin_testnet_fallback_rpc_endpoint,
        bitcoin_testnet_proxy_port: args.bitcoin_testnet_proxy_port,
        bitcoin_testnet_rpc_port: args.bitcoin_testnet_rpc_port,
        bitcoin_testnet_store_dir: args.bitcoin_testnet_store_dir,
        ethereum_holesky_consensus_http_port: args.ethereum_holesky_consensus_http_port,
        ethereum_holesky_consensus_metrics_port: args.ethereum_holesky_consensus_metrics_port,
        ethereum_holesky_consensus_p2p_port: args.ethereum_holesky_consensus_p2p_port,
        ethereum_holesky_consensus_store_dir: args.ethereum_holesky_consensus_store_dir,
        ethereum_holesky_execution_discovery_port: args.ethereum_holesky_execution_discovery_port,
        ethereum_holesky_execution_http_port: args.ethereum_holesky_execution_http_port,
        ethereum_holesky_execution_metrics_port: args.ethereum_holesky_execution_metrics_port,
        ethereum_holesky_execution_rpc_port: args.ethereum_holesky_execution_rpc_port,
        ethereum_holesky_execution_store_dir: args.ethereum_holesky_execution_store_dir,
        ethereum_holesky_fallback_rpc_endpoint: args.ethereum_holesky_fallback_rpc_endpoint,
        ethereum_mainnet_consensus_http_port: args.ethereum_mainnet_consensus_http_port,
        ethereum_mainnet_consensus_metrics_port: args.ethereum_mainnet_consensus_metrics_port,
        ethereum_mainnet_consensus_p2p_port: args.ethereum_mainnet_consensus_p2p_port,
        ethereum_mainnet_consensus_store_dir: args.ethereum_mainnet_consensus_store_dir,
        ethereum_mainnet_execution_discovery_port: args.ethereum_mainnet_execution_discovery_port,
        ethereum_mainnet_execution_http_port: args.ethereum_mainnet_execution_http_port,
        ethereum_mainnet_execution_metrics_port: args.ethereum_mainnet_execution_metrics_port,
        ethereum_mainnet_execution_rpc_port: args.ethereum_mainnet_execution_rpc_port,
        ethereum_mainnet_execution_store_dir: args.ethereum_mainnet_execution_store_dir,
        ethereum_mainnet_fallback_rpc_endpoint: args.ethereum_mainnet_fallback_rpc_endpoint,
        ethereum_sepolia_consensus_http_port: args.ethereum_sepolia_consensus_http_port,
        ethereum_sepolia_consensus_metrics_port: args.ethereum_sepolia_consensus_metrics_port,
        ethereum_sepolia_consensus_p2p_port: args.ethereum_sepolia_consensus_p2p_port,
        ethereum_sepolia_consensus_store_dir: args.ethereum_sepolia_consensus_store_dir,
        ethereum_sepolia_execution_discovery_port: args.ethereum_sepolia_execution_discovery_port,
        ethereum_sepolia_execution_http_port: args.ethereum_sepolia_execution_http_port,
        ethereum_sepolia_execution_metrics_port: args.ethereum_sepolia_execution_metrics_port,
        ethereum_sepolia_execution_rpc_port: args.ethereum_sepolia_execution_rpc_port,
        ethereum_sepolia_execution_store_dir: args.ethereum_sepolia_execution_store_dir,
        ethereum_sepolia_fallback_rpc_endpoint: args.ethereum_sepolia_fallback_rpc_endpoint,
        governance,
        p2p_port: args.p2p_port,
        port: args.port,
        nats_cli_bin_dir: args.nats_cli_bin_dir,
        nats_client_port: args.nats_client_port,
        nats_cluster_port: args.nats_cluster_port,
        nats_config_dir: args.nats_config_dir,
        nats_http_port: args.nats_http_port,
        nats_server_bin_dir: args.nats_server_bin_dir,
        nats_store_dir: args.nats_store_dir,
        network_config_path: args.network_config_path,
        node_key: private_key,
        postgres_bin_path: args.postgres_bin_path,
        postgres_port: args.postgres_port,
        postgres_skip_vacuum: args.skip_vacuum,
        postgres_store_dir: args.postgres_store_dir,
        radix_mainnet_fallback_rpc_endpoint: args.radix_mainnet_fallback_rpc_endpoint,
        radix_mainnet_http_port: args.radix_mainnet_http_port,
        radix_mainnet_p2p_port: args.radix_mainnet_p2p_port,
        radix_mainnet_store_dir: args.radix_mainnet_store_dir,
        radix_stokenet_fallback_rpc_endpoint: args.radix_stokenet_fallback_rpc_endpoint,
        radix_stokenet_http_port: args.radix_stokenet_http_port,
        radix_stokenet_p2p_port: args.radix_stokenet_p2p_port,
        radix_stokenet_store_dir: args.radix_stokenet_store_dir,
    })
}

#[tokio::main(worker_threads = 8)]
async fn main() -> Result<(), Error> {
    // Initialize tracing for better logging
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let config = create_node_config(args).await?;

    // Create shared shutdown token
    let shutdown_token = CancellationToken::new();

    // Set up signal handlers
    let signal_shutdown_token = shutdown_token.clone();
    tokio::spawn(async move {
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
            info!("Received interrupt signal");
        }

        info!("Shutting down");
        signal_shutdown_token.cancel();
    });

    // Run the node
    run_node(config, shutdown_token).await.map_err(Error::Local)
}
