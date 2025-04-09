use proven_postgres::{Postgres, PostgresOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for better logging
    tracing_subscriber::fmt::init();

    // Create a directory for storing Postgres data
    let store_dir = "./test-data".to_string();

    println!("Starting Postgres node in directory: {}", store_dir);

    // Create node options
    let options = PostgresOptions {
        bin_path: "/usr/local/pgsql/bin".to_string(),
        password: "postgres".to_string(),
        port: 5432,
        username: "postgres".to_string(),
        skip_vacuum: true,
        store_dir,
    };

    // Create and start the node
    let mut node = Postgres::new(options);
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
