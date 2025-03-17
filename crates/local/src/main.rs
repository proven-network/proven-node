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
    #[arg(long, default_value_t = 3200, env = "PROVEN_PORT")]
    port: u16,

    #[arg(long, default_value_t = 4222, env = "PROVEN_NATS_PORT")]
    nats_port: u16,

    /// Private key provided directly as an environment variable
    #[arg(long, env = "PROVEN_NODE_KEY", required = true)]
    node_key: String,

    #[arg(
        long,
        default_value = "/usr/local/pgsql/bin",
        env = "POSTGRES_BIN_PATH"
    )]
    postgres_bin_path: String,

    #[arg(
        long,
        default_value = "/tmp/proven/postgres",
        env = "POSTGRES_STORE_DIR"
    )]
    postgres_store_dir: PathBuf,

    #[arg(long, default_value_t = 30000, env = "PROVEN_RADIX_MAINNET_PORT")]
    radix_mainnet_port: u16,

    #[arg(
        long,
        default_value = "/tmp/proven/radix-node-mainnet",
        env = "PROVEN_RADIX_MAINNET_STORE_DIR"
    )]
    radix_mainnet_store_dir: PathBuf,

    #[arg(long, default_value_t = 30001, env = "PROVEN_RADIX_STOKENET_PORT")]
    radix_stokenet_port: u16,

    #[arg(
        long,
        default_value = "/tmp/proven/radix-node-stokenet",
        env = "PROVEN_RADIX_STOKENET_STORE_DIR"
    )]
    radix_stokenet_store_dir: PathBuf,

    /// Skip vacuuming the database
    #[arg(long, env = "PROVEN_SKIP_VACUUM")]
    skip_vacuum: bool,

    /// Use testnet
    #[arg(long, env = "PROVEN_TESTNET")]
    testnet: bool,

    /// Path to the topology file
    #[arg(long, default_value = "/etc/proven/topology.json")]
    topology_file: PathBuf,
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

    // Wait for either SIGINT (Ctrl-C) or SIGTERM
    if cfg!(unix) {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler failed");
        let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler failed");

        tokio::select! {
            _ = sigterm.recv() => info!("Received SIGTERM"),
            _ = sigint.recv() => info!("Received SIGINT"),
        }
    } else {
        // Fall back to just ctrl-c on non-unix platforms
        let _ = tokio::signal::ctrl_c().await;
    }

    info!("Shutting down");

    let () = node.shutdown().await;

    info!("Shutdown complete");

    Ok(())
}
