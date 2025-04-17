use std::path::PathBuf;

use proven_ethereum_reth::{EthereumNetwork, RethNode, RethNodeOptions};
use tracing::info;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for better logging
    tracing_subscriber::fmt::init();

    info!("Starting Reth node...");
    // Create and start the node
    let mut node = RethNode::new(RethNodeOptions {
        discovery_port: 30303,
        http_port: 8545,
        metrics_port: 9001,
        network: EthereumNetwork::Holesky,
        rpc_port: 8551,
        store_dir: PathBuf::from("./test-data"),
    });
    node.start().await?;
    info!("Reth node is ready!");

    // Keep the node running until user interrupts with Ctrl+C
    info!("\nNode is running. Press Ctrl+C to stop...");
    tokio::signal::ctrl_c().await?;

    // Shutdown the node when done
    info!("Shutting down Reth node...");
    node.shutdown().await?;
    info!("Node shutdown complete");

    Ok(())
}
