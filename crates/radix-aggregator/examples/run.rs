use proven_postgres::{Postgres, PostgresOptions};
use proven_radix_aggregator::{RadixAggregator, RadixAggregatorOptions};
use proven_radix_node::{RadixNode, RadixNodeOptions};
use radix_common::network::NetworkDefinition;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for better logging
    tracing_subscriber::fmt::init();

    println!("Starting Postgres...");
    let mut postgres = Postgres::new(PostgresOptions {
        password: "postgres".to_string(),
        port: 5432,
        username: "postgres".to_string(),
        skip_vacuum: true,
        store_dir: "/tmp/postgres-data".to_string(),
    });
    let _ = postgres.start().await?;
    println!("Postgres is ready!");

    println!("Starting Radix Node...");
    let mut radix_node = RadixNode::new(RadixNodeOptions {
        host_ip: fetch_external_ip().await,
        http_port: 3333,
        network_definition: NetworkDefinition::stokenet(),
        p2p_port: 30001,
        store_dir: "/tmp/radix-node-stokenet-data".to_string(),
        config_dir: "/tmp/radix-node-stokenet-config".to_string(),
    });
    let _ = radix_node.start().await?;
    println!("Radix Node is ready!");

    println!("Starting Radix Aggregator...");
    let mut aggregator = RadixAggregator::new(RadixAggregatorOptions {
        postgres_database: "radix_gateway".to_string(),
        postgres_ip_address: postgres.ip_address().to_string(),
        postgres_password: "postgres".to_string(),
        postgres_port: postgres.port(),
        postgres_username: "postgres".to_string(),
        radix_node_ip_address: radix_node.ip_address().to_string(),
        radix_node_port: radix_node.http_port(),
    });
    let _ = aggregator.start().await?;
    println!("Radix Aggregator is ready!");

    // Keep the aggregator running until user interrupts with Ctrl+C
    println!("\nAggregator is running. Press Ctrl+C to stop...");
    tokio::signal::ctrl_c().await?;

    // Shutdown the applications
    println!("Shutting down Radix Aggregator...");
    aggregator.shutdown().await?;
    println!("Aggregator shutdown complete");

    println!("Shutting down Radix Node...");
    radix_node.shutdown().await?;
    println!("Radix Node shutdown complete");

    println!("Shutting down Postgres...");
    postgres.shutdown().await?;
    println!("Postgres shutdown complete");

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
