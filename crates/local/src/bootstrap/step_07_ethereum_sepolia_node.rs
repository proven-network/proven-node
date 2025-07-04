//! Bootstrap Step 8: Ethereum Sepolia Node Initialization
//!
//! This step handles the initialization of Ethereum Sepolia testnet services, including:
//! - Conditional Ethereum Sepolia node startup based on specializations
//! - Reth execution client configuration and startup
//! - Lighthouse consensus client configuration and startup
//! - JWT authentication setup between execution and consensus clients

use super::Bootstrap;
use crate::error::Error;

use proven_bootable::Bootable;
use proven_ethereum_lighthouse::{
    EthereumNetwork as LighthouseNetwork, LighthouseNode, LighthouseNodeOptions,
};
use proven_ethereum_reth::{EthereumNetwork as RethNetwork, RethNode, RethNodeOptions};
use proven_governance::{Governance, NodeSpecialization};
use tracing::info;

pub async fn execute<G: Governance>(bootstrap: &mut Bootstrap<G>) -> Result<(), Error> {
    let network = bootstrap.network.as_ref().unwrap_or_else(|| {
        panic!("network not set before ethereum nodes step");
    });

    if network
        .specializations()
        .await?
        .contains(&NodeSpecialization::EthereumSepolia)
    {
        // Start Reth execution client
        let sepolia_reth_node = RethNode::new(RethNodeOptions {
            discovery_port: bootstrap.config.ethereum_sepolia_execution_discovery_port,
            http_port: bootstrap.config.ethereum_sepolia_execution_http_port,
            metrics_port: bootstrap.config.ethereum_sepolia_execution_metrics_port,
            network: RethNetwork::Sepolia,
            rpc_port: bootstrap.config.ethereum_sepolia_execution_rpc_port,
            store_dir: bootstrap
                .config
                .ethereum_sepolia_execution_store_dir
                .clone(),
        });

        sepolia_reth_node.start().await.map_err(Error::Bootable)?;

        let execution_rpc_jwt_hex = sepolia_reth_node.jwt_hex().await?;
        let execution_rpc_socket_addr = sepolia_reth_node.rpc_socket_addr().await?;

        info!("ethereum reth node (sepolia) started");

        // Start Lighthouse consensus client
        let sepolia_lighthouse_node = LighthouseNode::new(LighthouseNodeOptions {
            execution_rpc_jwt_hex,
            execution_rpc_socket_addr,
            host_ip: bootstrap.external_ip.to_string(),
            http_port: bootstrap.config.ethereum_sepolia_consensus_http_port,
            metrics_port: bootstrap.config.ethereum_sepolia_consensus_metrics_port,
            network: LighthouseNetwork::Sepolia,
            p2p_port: bootstrap.config.ethereum_sepolia_consensus_p2p_port,
            store_dir: bootstrap
                .config
                .ethereum_sepolia_consensus_store_dir
                .clone(),
        });

        // Add both Ethereum Sepolia nodes to bootables collection
        bootstrap.add_bootable(Box::new(sepolia_reth_node));
        bootstrap.add_bootable(Box::new(sepolia_lighthouse_node));

        info!("ethereum sepolia nodes started");
    }

    Ok(())
}
