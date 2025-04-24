use std::path::PathBuf;

use proven_attestation_mock::MockAttestor;
use proven_governance_mock::MockGovernance;
use proven_nats_server::{NatsServer, NatsServerOptions};
use proven_network::{ProvenNetwork, ProvenNetworkOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for better logging
    tracing_subscriber::fmt::init();

    // Create directories for storing NATS server data and config
    let store_dir = PathBuf::from("/tmp/nats-data");
    let config_dir = PathBuf::from("/tmp/nats-config");

    println!("Starting NATS server in directory: {}", store_dir.display());

    let private_key = ed25519_dalek::SigningKey::generate(&mut rand::thread_rng());

    // Create test network components
    let governance =
        MockGovernance::for_single_node(format!("http://localhost:{}", 3300), private_key.clone());
    let attestor = MockAttestor::new();
    let network = ProvenNetwork::new(ProvenNetworkOptions {
        attestor,
        governance,
        nats_cluster_port: 4222,
        private_key,
    })
    .await
    .expect("Failed to create network");

    // Get the path to the NATS server binary
    let bin_dir = match which::which("nats-server") {
        Ok(path) => path.parent().unwrap().to_path_buf(),
        Err(_) => PathBuf::from("/apps/nats/v2.11.0"),
    };

    // Create server options
    let options = NatsServerOptions {
        bin_dir: Some(bin_dir),
        client_port: 4222,
        config_dir,
        debug: true,
        http_port: 8222,
        network,
        server_name: "nats-example".to_string(),
        store_dir,
    };

    // Create and start the server
    let server = NatsServer::new(options).expect("Failed to create server");
    server.start().await?;

    println!("NATS server is ready!");
    println!("NATS URL: {}", server.get_client_url().await);

    // Connect a client to verify it's working
    server.build_client().await?;
    println!("Successfully connected client to NATS server");

    // Keep the server running until user interrupts with Ctrl+C
    println!("\nServer is running. Press Ctrl+C to stop...");
    tokio::signal::ctrl_c().await?;

    // Shutdown the server when done
    println!("Shutting down NATS server...");
    server.shutdown().await?;
    println!("Server shutdown complete");

    Ok(())
}
