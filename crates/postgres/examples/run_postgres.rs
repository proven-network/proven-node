use std::path::PathBuf;
use std::sync::Arc;

use proven_bootable::Bootable;
use proven_logger::{StdoutLogger, init};
use proven_postgres::{Postgres, PostgresOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize logger for better logging
    let logger = Arc::new(StdoutLogger::new());
    init(logger).expect("Failed to initialize logger");

    // Create a directory for storing Postgres data
    let store_dir = PathBuf::from("/tmp/postgres-data");

    println!(
        "Starting Postgres node in directory: {}",
        store_dir.display()
    );

    // Create node options
    let options = PostgresOptions {
        password: "postgres".to_string(),
        port: 5432,
        username: "postgres".to_string(),
        skip_vacuum: true,
        store_dir,
    };

    // Create and start the node
    let node = Postgres::new(options);
    node.start().await?;

    println!("Postgres node is ready!");

    // Keep the node running until user interrupts with Ctrl+C
    println!("\nNode is running. Press Ctrl+C to stop...");
    tokio::signal::ctrl_c().await?;

    // Shutdown the node when done
    println!("Shutting down Postgres node...");
    node.shutdown().await?;
    println!("Node shutdown complete");

    Ok(())
}
