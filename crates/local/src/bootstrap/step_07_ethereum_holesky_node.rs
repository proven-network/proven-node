//! Bootstrap Step 7: Ethereum Holesky Node Initialization
//!
//! This step handles the initialization of Ethereum Holesky testnet services, including:
//! - Conditional Ethereum Holesky node startup based on specializations
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
use proven_governance::NodeSpecialization;
use tracing::info;

pub async fn execute(bootstrap: &mut Bootstrap) -> Result<(), Error> {
    let network = bootstrap.network.as_ref().unwrap_or_else(|| {
        panic!("network not set before ethereum nodes step");
    });

    if network
        .specializations()
        .await?
        .contains(&NodeSpecialization::EthereumHolesky)
    {
        // Start Reth execution client
        let holesky_reth_node = RethNode::new(RethNodeOptions {
            discovery_port: bootstrap.config.ethereum_holesky_execution_discovery_port,
            http_port: bootstrap.config.ethereum_holesky_execution_http_port,
            metrics_port: bootstrap.config.ethereum_holesky_execution_metrics_port,
            network: RethNetwork::Holesky,
            rpc_port: bootstrap.config.ethereum_holesky_execution_rpc_port,
            store_dir: bootstrap
                .config
                .ethereum_holesky_execution_store_dir
                .clone(),
        });

        holesky_reth_node.start().await.map_err(Error::Bootable)?;

        let execution_rpc_jwt_hex = holesky_reth_node.jwt_hex().await?;
        let execution_rpc_socket_addr = holesky_reth_node.rpc_socket_addr().await?;

        info!("ethereum reth node (holesky) started");

        // Start Lighthouse consensus client
        let holesky_lighthouse_node = LighthouseNode::new(LighthouseNodeOptions {
            execution_rpc_jwt_hex,
            execution_rpc_socket_addr,
            host_ip: bootstrap.external_ip.to_string(),
            http_port: bootstrap.config.ethereum_holesky_consensus_http_port,
            metrics_port: bootstrap.config.ethereum_holesky_consensus_metrics_port,
            network: LighthouseNetwork::Holesky,
            p2p_port: bootstrap.config.ethereum_holesky_consensus_p2p_port,
            store_dir: bootstrap
                .config
                .ethereum_holesky_consensus_store_dir
                .clone(),
        });

        holesky_lighthouse_node
            .start()
            .await
            .map_err(Error::Bootable)?;

        // Add both Ethereum Holesky nodes to bootables collection
        bootstrap.add_bootable(Box::new(holesky_reth_node));
        bootstrap.add_bootable(Box::new(holesky_lighthouse_node));

        info!("ethereum holesky nodes started");
    }

    Ok(())
}
