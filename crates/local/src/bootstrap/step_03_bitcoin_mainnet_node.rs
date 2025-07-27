//! Bootstrap Step 4: Bitcoin Mainnet Node Initialization
//!
//! This step handles the initialization of Bitcoin mainnet services, including:
//! - Stream setup for Bitcoin mainnet proxy
//! - Conditional Bitcoin mainnet node startup based on specializations
//! - HTTP proxy service/client configuration
//! - RPC endpoint configuration

use super::Bootstrap;
use crate::error::Error;

use proven_bitcoin_core::{BitcoinNetwork, BitcoinNode, BitcoinNodeOptions};
use proven_bootable::Bootable;
use proven_http_proxy::{ProxyClient, ProxyService};
use proven_topology::{NodeSpecialization, TopologyAdaptor};
use tracing::info;
use url::Url;

#[allow(clippy::cognitive_complexity)]
pub async fn execute<G: TopologyAdaptor>(bootstrap: &mut Bootstrap<G>) -> Result<(), Error> {
    let node = bootstrap.node.as_ref().unwrap_or_else(|| {
        panic!("node not set before bitcoin mainnet node step");
    });

    // Get the engine client from bootstrap
    let engine_client = bootstrap
        .engine_client
        .as_ref()
        .expect("Engine client not available")
        .clone();

    if node
        .specializations()
        .contains(&NodeSpecialization::BitcoinMainnet)
    {
        // Start Bitcoin mainnet node
        let bitcoin_mainnet_node = BitcoinNode::new(BitcoinNodeOptions {
            bin_dir: None,
            network: BitcoinNetwork::Mainnet,
            store_dir: bootstrap.config.bitcoin_mainnet_store_dir.clone(),
            rpc_port: bootstrap.config.bitcoin_mainnet_rpc_port,
        })?;

        bitcoin_mainnet_node
            .start()
            .await
            .map_err(Error::Bootable)?;

        info!("bitcoin mainnet node started");

        let bitcoin_mainnet_proxy_service = ProxyService::new(
            engine_client,
            "bitcoin-mainnet-proxy",
            bitcoin_mainnet_node.rpc_socket_addr().await?,
        );

        bitcoin_mainnet_proxy_service
            .start()
            .await
            .map_err(Error::Bootable)?;

        bootstrap.bitcoin_mainnet_node_rpc_endpoint = bitcoin_mainnet_node.rpc_url().await?;

        // Add both services to bootables collection
        bootstrap.add_bootable(Box::new(bitcoin_mainnet_node));
        bootstrap.add_bootable(Box::new(bitcoin_mainnet_proxy_service));

        info!("bitcoin mainnet proxy service started");
    } else {
        let bitcoin_mainnet_proxy_client = ProxyClient::new(
            engine_client,
            "bitcoin-mainnet-proxy",
            bootstrap.config.bitcoin_mainnet_proxy_port,
        );

        bitcoin_mainnet_proxy_client
            .start()
            .await
            .map_err(Error::Bootable)?;

        bootstrap.bitcoin_mainnet_node_rpc_endpoint = Url::parse(&format!(
            "http://127.0.0.1:{}",
            bootstrap.config.bitcoin_mainnet_proxy_port
        ))
        .unwrap();

        // Add proxy client to bootables collection
        bootstrap.add_bootable(Box::new(bitcoin_mainnet_proxy_client));

        info!("bitcoin mainnet proxy client started");
    }

    Ok(())
}
