use std::path::PathBuf;

use proven_bootable::Bootable;
use proven_ethereum_reth::{EthereumNetwork, RethNode, RethNodeOptions};
use proven_logger::{StdoutLogger, info, init};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize logger for better logging
    let logger = Arc::new(StdoutLogger::new());
    init(logger).expect("Failed to initialize logger");

    info!("Starting Reth node...");
    // Create and start the node
    let node = RethNode::new(RethNodeOptions {
        discovery_port: 30303,
        http_port: 8545,
        metrics_port: 9001,
        network: EthereumNetwork::Holesky,
        rpc_port: 8551,
        store_dir: PathBuf::from("/tmp/ethereum-reth-holesky"),
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
