#![allow(deprecated)]

use std::env;
use std::path::PathBuf;

use helios_ethereum::config::networks::Network;
use proven_governance::Governance;
use proven_governance_helios::{HeliosGovernance, HeliosGovernanceOptions};
use tracing::info;

#[tokio::main]
async fn main() {
    // Load environment variables from .env file
    dotenv::dotenv().ok();

    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::DEBUG)
            .finish(),
    )
    .unwrap();

    // Get environment variables with fallbacks
    let network = match env::var("HELIOS_NETWORK").unwrap().as_str() {
        "mainnet" => Network::Mainnet,
        "sepolia" => Network::Sepolia,
        _ => panic!("Invalid network"),
    };
    let eth_rpc_url = env::var("ETH_RPC_URL").unwrap();
    let consensus_rpc = env::var("CONSENSUS_RPC_URL").unwrap();

    let token_address = env::var("TOKEN_CONTRACT_ADDRESS").unwrap();
    let node_governance_address = env::var("NODE_GOVERNANCE_CONTRACT_ADDRESS").unwrap();
    let version_governance_address = env::var("VERSION_GOVERNANCE_CONTRACT_ADDRESS").unwrap();

    info!("Using contract addresses:");
    info!("Token: {}", token_address);
    info!("Node Governance: {}", node_governance_address);
    info!("Version Governance: {}", version_governance_address);
    info!("Consensus RPC URL: {}", consensus_rpc);

    // Construct the governance options
    let options = HeliosGovernanceOptions {
        consensus_rpc,
        data_dir: PathBuf::from("/tmp/helios").join(network.to_string()),
        execution_rpc: eth_rpc_url,
        network,
        node_governance_contract_address: node_governance_address,
        token_contract_address: token_address,
        version_governance_contract_address: version_governance_address,
    };

    // Create and initialize the governance client
    let governance = HeliosGovernance::new(options).await.unwrap();
    info!("Governance client initialized");

    // Get active versions
    match governance.get_active_versions().await {
        Ok(versions) => {
            info!("Found {} active versions:", versions.len());
            for version in versions {
                info!(
                    "Version {}: PCR0: {}, PCR1: {}, PCR2: {}",
                    version.sequence,
                    hex::encode(version.ne_pcr0),
                    hex::encode(version.ne_pcr1),
                    hex::encode(version.ne_pcr2)
                );
            }
        }
        Err(e) => {
            info!("Error getting active versions: {:?}", e);
        }
    }

    // Get network topology
    match governance.get_topology().await {
        Ok(nodes) => {
            info!("Found {} nodes in the network:", nodes.len());
            for node in nodes {
                info!(
                    "Node {}: Region: {}, AZ: {}, Origin: {}, Specializations: {:?}",
                    node.public_key,
                    node.region,
                    node.availability_zone,
                    node.origin,
                    node.specializations
                );
            }
        }
        Err(e) => {
            info!("Error getting network topology: {:?}", e);
        }
    }
}
