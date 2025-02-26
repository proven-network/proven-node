#![allow(deprecated)]

use std::path::PathBuf;

use proven_governance::Governance;
use proven_governance_helios::{HeliosGovernance, HeliosGovernanceOptions};
use tracing::info;

#[tokio::main]
async fn main() {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::DEBUG)
            .finish(),
    )
    .unwrap();

    let eth_rpc_url = "https://eth-mainnet.g.alchemy.com/v2/_82Y5NUN4Ed9769-SfQbd0AVgSd69448";
    let consensus_rpc = "https://www.lightclientdata.org";
    info!("Consensus RPC URL: {}", consensus_rpc);

    // Construct the governance options
    let options = HeliosGovernanceOptions {
        consensus_rpc: consensus_rpc.to_string(),
        data_dir: PathBuf::from("/tmp/helios"),
        execution_rpc: eth_rpc_url.to_string(),
        // These are placeholder addresses
        node_governance_contract_address: "0x1234567890123456789012345678901234567890".to_string(),
        token_contract_address: "0x1234567890123456789012345678901234567890".to_string(),
        version_governance_contract_address: "0x1234567890123456789012345678901234567890"
            .to_string(),
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
                    version.sequence, version.ne_pcr0, version.ne_pcr1, version.ne_pcr2
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
                    "Node {}: Region: {}, AZ: {}, FQDN: {}, Specializations: {:?}",
                    node.id, node.region, node.availability_zone, node.fqdn, node.specializations
                );
            }
        }
        Err(e) => {
            info!("Error getting network topology: {:?}", e);
        }
    }
}
