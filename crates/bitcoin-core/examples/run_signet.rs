use proven_bitcoin_core::{BitcoinNetwork, BitcoinNode, BitcoinNodeOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for better logging
    tracing_subscriber::fmt::init();

    // Create a directory for storing Bitcoin Core data
    // Note: For a persistent node, you might want to use a fixed directory instead of a temporary one
    let store_dir = "/tmp/bitcoin-core-signet".to_string();

    println!("Starting Bitcoin Core node in directory: {}", store_dir);

    // Create node options with Signet network
    let options = BitcoinNodeOptions {
        network: BitcoinNetwork::Signet,
        store_dir,
        rpc_port: None, // Use default (8332)
    };

    // Create and start the node
    let mut node = BitcoinNode::new(options);
    node.start().await?;

    println!("Bitcoin Core node is ready!");

    // Keep the node running until user interrupts with Ctrl+C
    println!("\nNode is running on Signet. Press Ctrl+C to stop...");
    tokio::signal::ctrl_c().await?;

    // Shutdown the node when done
    println!("Shutting down Bitcoin Core node...");
    node.shutdown().await?;
    println!("Node shutdown complete");

    Ok(())
}
