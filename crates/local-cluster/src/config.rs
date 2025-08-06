//! Configuration builders for cluster nodes

use ed25519_dalek::SigningKey;
use proven_topology_mock::MockTopologyAdaptor;
use proven_util::port_allocator::allocate_port;
use std::path::PathBuf;
use std::sync::Arc;
use url::Url;

/// Type alias for node configuration
pub type NodeConfig = proven_local::NodeConfig<MockTopologyAdaptor>;

/// Build a complete node configuration
#[allow(clippy::too_many_lines)]
pub fn build_node_config(
    name: &str,
    main_port: u16,
    governance: &Arc<MockTopologyAdaptor>,
    private_key: SigningKey,
    session_id: &str,
) -> NodeConfig {
    NodeConfig {
        allow_single_node: false,
        bitcoin_mainnet_fallback_rpc_endpoint: Url::parse("https://bitcoin-rpc.publicnode.com")
            .unwrap(),
        bitcoin_mainnet_proxy_port: allocate_port(),
        bitcoin_mainnet_store_dir: PathBuf::from(format!(
            "/tmp/proven/{session_id}/data/{name}/bitcoin-mainnet"
        )),
        bitcoin_mainnet_rpc_port: allocate_port(),

        bitcoin_testnet_fallback_rpc_endpoint: Url::parse(
            "https://bitcoin-testnet-rpc.publicnode.com",
        )
        .unwrap(),
        bitcoin_testnet_proxy_port: allocate_port(),
        bitcoin_testnet_store_dir: PathBuf::from(format!(
            "/tmp/proven/{session_id}/data/{name}/bitcoin-testnet"
        )),
        bitcoin_testnet_rpc_port: allocate_port(),

        ethereum_holesky_consensus_http_port: allocate_port(),
        ethereum_holesky_consensus_metrics_port: allocate_port(),
        ethereum_holesky_consensus_p2p_port: allocate_port(),
        ethereum_holesky_consensus_store_dir: PathBuf::from(format!(
            "/tmp/proven/{session_id}/data/{name}/ethereum-holesky/lighthouse"
        )),

        ethereum_holesky_execution_discovery_port: allocate_port(),
        ethereum_holesky_execution_http_port: allocate_port(),
        ethereum_holesky_execution_metrics_port: allocate_port(),
        ethereum_holesky_execution_rpc_port: allocate_port(),
        ethereum_holesky_execution_store_dir: PathBuf::from(format!(
            "/tmp/proven/{session_id}/data/{name}/ethereum-holesky/reth"
        )),
        ethereum_holesky_fallback_rpc_endpoint: Url::parse(
            "https://ethereum-holesky-rpc.publicnode.com",
        )
        .unwrap(),

        ethereum_mainnet_consensus_http_port: allocate_port(),
        ethereum_mainnet_consensus_metrics_port: allocate_port(),
        ethereum_mainnet_consensus_p2p_port: allocate_port(),
        ethereum_mainnet_consensus_store_dir: PathBuf::from(format!(
            "/tmp/proven/{session_id}/data/{name}/ethereum-mainnet/lighthouse"
        )),

        ethereum_mainnet_execution_discovery_port: allocate_port(),
        ethereum_mainnet_execution_http_port: allocate_port(),
        ethereum_mainnet_execution_metrics_port: allocate_port(),
        ethereum_mainnet_execution_rpc_port: allocate_port(),
        ethereum_mainnet_execution_store_dir: PathBuf::from(format!(
            "/tmp/proven/{session_id}/data/{name}/ethereum-mainnet/reth"
        )),
        ethereum_mainnet_fallback_rpc_endpoint: Url::parse("https://ethereum-rpc.publicnode.com")
            .unwrap(),

        ethereum_sepolia_consensus_http_port: allocate_port(),
        ethereum_sepolia_consensus_metrics_port: allocate_port(),
        ethereum_sepolia_consensus_p2p_port: allocate_port(),
        ethereum_sepolia_consensus_store_dir: PathBuf::from(format!(
            "/tmp/proven/{session_id}/data/{name}/ethereum-sepolia/lighthouse"
        )),

        ethereum_sepolia_execution_discovery_port: allocate_port(),
        ethereum_sepolia_execution_http_port: allocate_port(),
        ethereum_sepolia_execution_metrics_port: allocate_port(),
        ethereum_sepolia_execution_rpc_port: allocate_port(),
        ethereum_sepolia_execution_store_dir: PathBuf::from(format!(
            "/tmp/proven/{session_id}/data/{name}/ethereum-sepolia/reth"
        )),
        ethereum_sepolia_fallback_rpc_endpoint: Url::parse(
            "https://ethereum-sepolia-rpc.publicnode.com",
        )
        .unwrap(),

        governance: (**governance).clone(),
        port: main_port,

        network_config_path: None,
        node_key: private_key,

        postgres_bin_path: PathBuf::from("/usr/local/pgsql/bin"),
        postgres_port: allocate_port(),
        postgres_skip_vacuum: false,
        postgres_store_dir: PathBuf::from(format!("/tmp/proven/{session_id}/data/{name}/postgres")),

        radix_mainnet_fallback_rpc_endpoint: Url::parse("https://mainnet.radixdlt.com").unwrap(),
        radix_mainnet_http_port: allocate_port(),
        radix_mainnet_p2p_port: allocate_port(),
        radix_mainnet_store_dir: PathBuf::from(format!(
            "/tmp/proven/{session_id}/data/{name}/radix-node-mainnet"
        )),

        radix_stokenet_fallback_rpc_endpoint: Url::parse("https://stokenet.radixdlt.com").unwrap(),
        radix_stokenet_http_port: allocate_port(),
        radix_stokenet_p2p_port: allocate_port(),
        radix_stokenet_store_dir: PathBuf::from(format!(
            "/tmp/proven/{session_id}/data/{name}/radix-node-stokenet"
        )),

        rocksdb_store_dir: PathBuf::from(format!("/tmp/proven/{session_id}/data/{name}/rocksdb")),
    }
}

/// Create a default node configuration
pub fn create_node_config(
    _execution_order: u32,
    name: &str,
    port: u16,
    governance: &Arc<MockTopologyAdaptor>,
    session_id: &str,
    private_key: SigningKey,
) -> NodeConfig {
    build_node_config(name, port, governance, private_key, session_id)
}
