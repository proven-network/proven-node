use std::path::PathBuf;

use proven_ethereum_lighthouse::{EthereumNetwork, LighthouseNode, LighthouseNodeOptions};
use proven_ethereum_reth::{EthereumNetwork as RethNetwork, RethNode, RethNodeOptions};
use tracing::info;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for better logging
    tracing_subscriber::fmt::init();

    info!("Starting Reth node...");
    let mut reth_node = RethNode::new(RethNodeOptions {
        discovery_port: 30304,
        http_port: 8545,
        metrics_port: 9001,
        network: RethNetwork::Holesky,
        rpc_port: 8551,
        store_dir: PathBuf::from("../ethereum-reth/test-data"),
    });
    reth_node.start().await?;
    info!("Reth node is ready!");

    info!("Starting Lighthouse node...");
    let mut lighthouse_node = LighthouseNode::new(LighthouseNodeOptions {
        execution_rpc_ip_address: reth_node.ip_address().to_string(),
        execution_rpc_jwt_hex: reth_node.jwt_hex().await?,
        execution_rpc_port: reth_node.rpc_port(),
        host_ip: fetch_external_ip().await,
        http_port: 5052,
        metrics_port: 5054,
        network: EthereumNetwork::Holesky,
        p2p_port: 10109,
        store_dir: PathBuf::from("./test-data"),
    });
    lighthouse_node.start().await?;
    info!("Lighthouse node is ready!");

    // Keep the node running until user interrupts with Ctrl+C
    info!("\nNode is running. Press Ctrl+C to stop...");
    tokio::signal::ctrl_c().await?;

    // Shutdown the node when done
    info!("Shutting down Lighthouse node...");
    lighthouse_node.shutdown().await?;
    info!("Lighthouse node shutdown complete");

    info!("Shutting down Reth node...");
    reth_node.shutdown().await?;
    info!("Reth node shutdown complete");

    Ok(())
}

/// Fetches the external IP address using myip.com API
async fn fetch_external_ip() -> String {
    let response = reqwest::get("https://api.myip.com").await.unwrap();

    let json_response = response.json::<serde_json::Value>().await.unwrap();

    let ip_address = json_response["ip"].as_str().unwrap().to_string();

    info!("External IP detected: {}", ip_address);

    ip_address
}
