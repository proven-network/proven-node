//! Bootstrap Step 4: Bitcoin Mainnet Node Initialization
//!
//! This step handles the initialization of Bitcoin mainnet services, including:
//! - NATS stream setup for Bitcoin mainnet proxy
//! - Conditional Bitcoin mainnet node startup based on specializations
//! - HTTP proxy service/client configuration
//! - RPC endpoint configuration

use super::Bootstrap;
use crate::error::{Error, Result};

use proven_bitcoin_core::{BitcoinNetwork, BitcoinNode, BitcoinNodeOptions};
use proven_bootable::Bootable;
use proven_governance::NodeSpecialization;
use proven_http_proxy::{
    HttpProxyClient, HttpProxyClientOptions, HttpProxyService, HttpProxyServiceOptions,
};
use proven_messaging::stream::Stream;
use proven_messaging_nats::client::NatsClientOptions;
use proven_messaging_nats::service::NatsServiceOptions;
use proven_messaging_nats::stream::{NatsStream, NatsStreamOptions};
use tracing::info;
use url::Url;

pub async fn execute(bootstrap: &mut Bootstrap) -> Result<()> {
    let nats_client = bootstrap.nats_client.as_ref().unwrap_or_else(|| {
        panic!("nats client not fetched before bitcoin mainnet node step");
    });

    let network = bootstrap.network.as_ref().unwrap_or_else(|| {
        panic!("network not set before bitcoin mainnet node step");
    });

    let bitcoin_mainnet_proxy_stream = NatsStream::new(
        "BITCOIN_MAINNET_PROXY",
        NatsStreamOptions {
            client: nats_client.clone(),
            num_replicas: bootstrap.num_replicas,
        },
    )
    .init()
    .await
    .map_err(|e| Error::Stream(e.to_string()))?;

    if network
        .specializations()
        .await?
        .contains(&NodeSpecialization::BitcoinMainnet)
    {
        // Start Bitcoin mainnet node
        let bitcoin_mainnet_node = BitcoinNode::new(BitcoinNodeOptions {
            network: BitcoinNetwork::Mainnet,
            store_dir: bootstrap.args.bitcoin_mainnet_store_dir.clone(),
            rpc_port: None,
        });

        bitcoin_mainnet_node
            .start()
            .await
            .map_err(Error::Bootable)?;

        info!("bitcoin mainnet node started");

        let bitcoin_mainnet_proxy_service = HttpProxyService::new(HttpProxyServiceOptions {
            service_options: NatsServiceOptions {
                client: nats_client.clone(),
                durable_name: None,
                jetstream_context: async_nats::jetstream::new(nats_client.clone()),
            },
            stream: bitcoin_mainnet_proxy_stream,
            target_addr: bitcoin_mainnet_node.rpc_socket_addr().await?,
        })
        .await?;

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
        let bitcoin_mainnet_proxy_client = HttpProxyClient::new(HttpProxyClientOptions {
            client_options: NatsClientOptions {
                client: nats_client.clone(),
            },
            http_port: bootstrap.args.bitcoin_mainnet_proxy_port,
            stream: bitcoin_mainnet_proxy_stream,
        });

        bitcoin_mainnet_proxy_client
            .start()
            .await
            .map_err(Error::Bootable)?;

        bootstrap.bitcoin_mainnet_node_rpc_endpoint = Url::parse(&format!(
            "http://127.0.0.1:{}",
            bootstrap.args.bitcoin_mainnet_proxy_port
        ))
        .unwrap();

        // Add proxy client to bootables collection
        bootstrap.add_bootable(Box::new(bitcoin_mainnet_proxy_client));

        info!("bitcoin mainnet proxy client started");
    }

    Ok(())
}
