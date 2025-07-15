//! WAL server binary for S3 storage
//!
//! This server runs on the host and provides WAL services to the enclave
//! for durability guarantees.

use clap::Parser;
use proven_storage_s3::wal::server::{FileBasedWalHandler, WalServer};
use proven_vsock_rpc::VsockAddr;
use std::path::PathBuf;
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(author, version, about = "WAL server for S3 storage", long_about = None)]
struct Args {
    /// VSOCK port to listen on
    #[arg(short, long, default_value_t = 5000)]
    port: u32,

    /// Directory for WAL storage
    #[arg(short, long, default_value = "/var/lib/proven/wal")]
    wal_dir: PathBuf,

    /// Enable debug logging
    #[arg(short, long)]
    debug: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize logging
    let log_level = if args.debug {
        tracing::Level::DEBUG
    } else {
        tracing::Level::INFO
    };

    tracing_subscriber::fmt().with_max_level(log_level).init();

    info!(
        "Starting WAL server on port {} with data directory: {:?}",
        args.port, args.wal_dir
    );

    // Create WAL handler
    let handler = FileBasedWalHandler::new(&args.wal_dir).await?;

    // Create server
    let addr = VsockAddr::new(3, args.port); // CID 3 for host
    let server = WalServer::new(addr, handler);

    // Start serving
    info!("WAL server listening on {:?}", addr);

    if let Err(e) = server.serve().await {
        error!("WAL server error: {}", e);
        return Err(e.into());
    }

    Ok(())
}
