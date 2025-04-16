use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::str::FromStr;

use proven_ethereum_lighthouse::{EthereumNetwork, LighthouseNode, LighthouseNodeOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for better logging
    tracing_subscriber::fmt::init();

    // Create a directory for storing Lighthouse data
    let store_dir = PathBuf::from("./test-data");

    println!(
        "Starting Lighthouse node in directory: {}",
        store_dir.display()
    );

    // Create node options
    let options = LighthouseNodeOptions {
        execution_rpc_addr: SocketAddrV4::from_str("127.0.0.1:8551")?,
        host_ip: "127.0.0.1".to_string(),
        http_addr: SocketAddrV4::from_str("127.0.0.1:5052")?,
        metrics_addr: SocketAddrV4::from_str("127.0.0.1:5054")?,
        network: EthereumNetwork::Holesky,
        p2p_addr: SocketAddrV4::from_str("127.0.0.1:9000")?,
        store_dir,
    };

    // Create and start the node
    let mut node = LighthouseNode::new(options);
    node.start().await?;

    println!("Lighthouse node is ready!");

    // Keep the node running until user interrupts with Ctrl+C
    println!("\nNode is running. Press Ctrl+C to stop...");
    tokio::signal::ctrl_c().await?;

    // Shutdown the node when done
    println!("Shutting down Lighthouse node...");
    node.shutdown().await?;
    println!("Node shutdown complete");

    Ok(())
}
