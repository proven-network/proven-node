use std::convert::Infallible;
use std::path::PathBuf;

use bytes::Bytes;
use proven_attestation::Attestor;
use proven_attestation_mock::MockAttestor;
use proven_bootable::Bootable;
use proven_governance::Version;
use proven_governance_mock::MockGovernance;
use proven_nats_server::{NatsServer, NatsServerOptions};
use proven_network::{ProvenNetwork, ProvenNetworkOptions};
use proven_store_memory::MemoryStore;

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
    let attestor = MockAttestor::new();
    // Just use single version based on mock attestation pcrs (deterministic hashes on cargo version)
    let pcrs = attestor.pcrs().await.unwrap();
    let version = Version::from_pcrs(pcrs);
    let governance = MockGovernance::for_single_node(
        format!("http://localhost:{}", 3300),
        private_key.clone(),
        version,
    );
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
        Err(_) => PathBuf::from("/apps/nats/v2.11.4"),
    };

    // Create server options
    let options: NatsServerOptions<
        MockGovernance,
        MockAttestor,
        MemoryStore<Bytes, Infallible, Infallible>,
    > = NatsServerOptions {
        bin_dir: Some(bin_dir),
        cert_store: None,
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
    let client = server.build_client().await?;
    println!("Successfully connected client to NATS server");

    // Publish a message to the server
    client.publish("test", Bytes::from("Hello, world!")).await?;
    println!("Published message to NATS server");

    // Keep the server running until user interrupts with Ctrl+C
    println!("\nServer is running. Press Ctrl+C to stop...");
    tokio::signal::ctrl_c().await?;

    drop(client);

    // Shutdown the server when done
    println!("Shutting down NATS server...");
    server.shutdown().await?;
    println!("Server shutdown complete");

    Ok(())
}
