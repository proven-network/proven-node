use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::str::FromStr;

use proven_ethereum_reth::{EthereumNetwork, RethNode, RethNodeOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for better logging
    tracing_subscriber::fmt::init();

    // Create a directory for storing Reth data
    let store_dir = PathBuf::from("./test-data");

    println!("Starting Reth node in directory: {}", store_dir.display());

    // Create node options
    let options = RethNodeOptions {
        discovery_addr: SocketAddrV4::from_str("127.0.0.1:30303")?,
        http_addr: SocketAddrV4::from_str("127.0.0.1:8545")?,
        metrics_addr: SocketAddrV4::from_str("127.0.0.1:9001")?,
        network: EthereumNetwork::Holesky,
        rpc_addr: SocketAddrV4::from_str("127.0.0.1:8551")?,
        store_dir,
    };

    // Create and start the node
    let mut node = RethNode::new(options);
    node.start().await?;

    println!("Reth node is ready!");

    // Keep the node running until user interrupts with Ctrl+C
    println!("\nNode is running. Press Ctrl+C to stop...");
    tokio::signal::ctrl_c().await?;

    // Shutdown the node when done
    println!("Shutting down Reth node...");
    node.shutdown().await?;
    println!("Node shutdown complete");

    Ok(())
}
