//! Bootstrap Step 5: Bitcoin Testnet Node Initialization
//!
//! This step handles the initialization of Bitcoin testnet services, including:
//! - Stream setup for Bitcoin testnet proxy
//! - Conditional Bitcoin testnet node startup based on specializations
//! - HTTP proxy service/client configuration
//! - RPC endpoint configuration

use super::Bootstrap;
use crate::error::Error;

use proven_bitcoin_core::{BitcoinNetwork, BitcoinNode, BitcoinNodeOptions};
use proven_bootable::Bootable;
use proven_http_proxy::{
    HttpProxyClient, HttpProxyClientOptions, HttpProxyService, HttpProxyServiceOptions,
};
use proven_messaging::stream::Stream;
use proven_messaging_memory::client::MemoryClientOptions;
use proven_messaging_memory::service::MemoryServiceOptions;
use proven_messaging_memory::stream::{MemoryStream, MemoryStreamOptions};
use proven_topology::{NodeSpecialization, TopologyAdaptor};
use tracing::info;
use url::Url;

#[allow(clippy::cognitive_complexity)]
pub async fn execute<G: TopologyAdaptor>(bootstrap: &mut Bootstrap<G>) -> Result<(), Error> {
    let node = bootstrap.node.as_ref().unwrap_or_else(|| {
        panic!("node not set before bitcoin testnet node step");
    });

    let bitcoin_testnet_proxy_stream =
        MemoryStream::new("BITCOIN_TESTNET_PROXY", MemoryStreamOptions)
            .init()
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

    if node
        .specializations()
        .contains(&NodeSpecialization::BitcoinTestnet)
    {
        // Start Bitcoin testnet node
        let bitcoin_testnet_node = BitcoinNode::new(BitcoinNodeOptions {
            bin_dir: None,
            network: BitcoinNetwork::Testnet,
            store_dir: bootstrap.config.bitcoin_testnet_store_dir.clone(),
            rpc_port: bootstrap.config.bitcoin_testnet_rpc_port,
        })?;

        bitcoin_testnet_node
            .start()
            .await
            .map_err(Error::Bootable)?;

        info!("bitcoin testnet node started");

        let bitcoin_testnet_proxy_service = HttpProxyService::new(HttpProxyServiceOptions {
            service_options: MemoryServiceOptions,
            stream: bitcoin_testnet_proxy_stream,
            target_addr: bitcoin_testnet_node.rpc_socket_addr().await?,
        })
        .await?;

        bitcoin_testnet_proxy_service
            .start()
            .await
            .map_err(Error::Bootable)?;

        bootstrap.bitcoin_testnet_node_rpc_endpoint = bitcoin_testnet_node.rpc_url().await?;

        // Add both services to bootables collection
        bootstrap.add_bootable(Box::new(bitcoin_testnet_node));
        bootstrap.add_bootable(Box::new(bitcoin_testnet_proxy_service));

        info!("bitcoin testnet proxy service started");
    } else {
        let bitcoin_testnet_proxy_client = HttpProxyClient::new(HttpProxyClientOptions {
            client_options: MemoryClientOptions,
            http_port: bootstrap.config.bitcoin_testnet_proxy_port,
            stream: bitcoin_testnet_proxy_stream,
        });

        bitcoin_testnet_proxy_client
            .start()
            .await
            .map_err(Error::Bootable)?;

        bootstrap.bitcoin_testnet_node_rpc_endpoint = Url::parse(&format!(
            "http://127.0.0.1:{}",
            bootstrap.config.bitcoin_testnet_proxy_port
        ))
        .unwrap();

        // Add proxy client to bootables collection
        bootstrap.add_bootable(Box::new(bitcoin_testnet_proxy_client));

        info!("bitcoin testnet proxy client started");
    }

    Ok(())
}
