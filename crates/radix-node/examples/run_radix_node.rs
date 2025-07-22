use std::path::PathBuf;

use proven_bootable::Bootable;
use proven_radix_node::{RadixNode, RadixNodeOptions};
use radix_common::network::NetworkDefinition;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing for better logging
    tracing_subscriber::fmt::init();

    // Create directories for storing Radix Node data and configuration
    let store_dir = PathBuf::from("/tmp/radix-node-stokenet-data");
    let config_dir = PathBuf::from("/tmp/radix-node-stokenet-config");

    println!(
        "Starting Radix Node with data in '{}' and config in '{}'",
        store_dir.display(),
        config_dir.display()
    );

    // Create and start the node
    let node = RadixNode::new(RadixNodeOptions {
        host_ip: fetch_external_ip().await,
        http_port: 3333,
        network_definition: NetworkDefinition::stokenet(),
        p2p_port: 30001,
        store_dir,
        config_dir,
    });
    node.start().await?;

    println!("Radix Node is ready!");

    // Keep the node running until user interrupts with Ctrl+C
    println!("\nNode is running. Press Ctrl+C to stop...");
    tokio::signal::ctrl_c().await?;

    // Shutdown the node when done
    println!("Shutting down Radix Node...");
    node.shutdown().await?;
    println!("Node shutdown complete");

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
