//! Binary to bootstrap other components locally.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod bootstrap;
mod error;
mod hosts;
mod net;
mod node;

use bootstrap::Bootstrap;
use error::Result;

use std::path::PathBuf;

use clap::Parser;
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value_t = 3200)]
    port: u16,

    #[arg(long, default_value_t = 4222, env = "NATS_PORT")]
    nats_port: u16,

    #[arg(long, default_value_t = 30000, env = "RADIX_MAINNET_PORT")]
    radix_mainnet_port: u16,

    #[arg(
        long,
        default_value = "/tmp/proven/radix-node-mainnet",
        env = "RADIX_MAINNET_STORE_DIR"
    )]
    radix_mainnet_store_dir: PathBuf,

    #[arg(long, default_value_t = 30001, env = "RADIX_STOKENET_PORT")]
    radix_stokenet_port: u16,

    #[arg(
        long,
        default_value = "/tmp/proven/radix-node-stokenet",
        env = "RADIX_STOKENET_STORE_DIR"
    )]
    radix_stokenet_store_dir: PathBuf,

    /// Skip vacuuming the database
    #[arg(long)]
    skip_vacuum: bool,

    /// Use testnet
    #[arg(long)]
    testnet: bool,

    /// Path to the topology file
    #[arg(long, default_value = "/etc/proven/topology.json")]
    topology_file: PathBuf,

    /// Private key provided directly as an environment variable
    #[arg(long, env = "PRIVATE_KEY", required = true)]
    private_key: String,
}

#[tokio::main(worker_threads = 8)]
async fn main() -> Result<()> {
    tracing::subscriber::set_global_default(
        FmtSubscriber::builder()
            .with_max_level(Level::INFO)
            .finish(),
    )?;

    let args = Args::parse();

    let bootstrap = Bootstrap::new(args).await?;
    let node = bootstrap.initialize().await?;

    let _ = tokio::signal::ctrl_c().await;

    info!("Shutting down");

    let () = node.shutdown().await;

    info!("Shutdown complete");

    Ok(())
}
