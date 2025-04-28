use proven_bitcoin_core::{BitcoinNetwork, BitcoinNode, BitcoinNodeOptions};
use proven_bootable::Bootable;
use serde_json::Value;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for better logging
    tracing_subscriber::fmt::init();

    // Create a temporary directory for storing Bitcoin Core data
    let temp_dir = tempfile::tempdir()?;
    let store_dir = temp_dir.into_path();

    println!(
        "Starting Bitcoin Core node in directory: {}",
        store_dir.display()
    );

    // Create node options with regtest network for faster testing
    let options = BitcoinNodeOptions {
        network: BitcoinNetwork::Regtest, // Use regtest for quick startup
        store_dir,
        rpc_port: None, // Use default (8332)
    };

    // Create and start the node
    let node = BitcoinNode::new(options);
    node.start().await?;

    println!("Bitcoin Core node is ready!");

    // Make an RPC call to get blockchain info with explicit type annotations
    let blockchain_info: Value = node
        .rpc_call::<Vec<String>, Value>("getblockchaininfo", vec![])
        .await?;
    println!("Blockchain info: {:#?}", blockchain_info);

    // Create a wallet to get a valid address
    println!("Creating a new wallet...");
    let wallet_name = "testwallet";
    let _create_wallet: Value = node
        .rpc_call::<Vec<String>, Value>("createwallet", vec![wallet_name.to_string()])
        .await?;

    // Generate a new address
    println!("Generating a new address...");
    let address: String = node
        .rpc_call::<Vec<String>, String>("getnewaddress", vec![])
        .await?;
    println!("Generated address: {}", address);

    // Generate some blocks in regtest mode
    println!("Generating blocks...");
    let params = serde_json::json!([10, address]);
    let blocks: Value = node
        .rpc_call::<Value, Value>("generatetoaddress", params)
        .await?;
    println!("Generated blocks: {:#?}", blocks);

    // Get updated blockchain info
    let updated_info: Value = node
        .rpc_call::<Vec<String>, Value>("getblockchaininfo", vec![])
        .await?;
    println!("Updated blockchain info: {:#?}", updated_info);

    // Shutdown the node when done
    println!("Shutting down Bitcoin Core node...");
    node.shutdown().await?;
    println!("Node shutdown complete");

    Ok(())
}
