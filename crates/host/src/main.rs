//! Binary to run on the host machine to manage enclave lifecycle.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod commands;
mod error;
mod net;

/// Client for interacting with `nitro-cli`.
pub mod nitro;

/// Client for interacting with `systemctl`.
pub mod systemctl;

use error::{Error, Result};

use std::net::Ipv4Addr;
use std::path::PathBuf;

use cidr::Ipv4Cidr;
use clap::{arg, command, Parser};

#[tokio::main(worker_threads = 12)]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Initialize(args) => commands::initialize(*args).await,
        Commands::Connect(args) => commands::connect(*args).await,
        Commands::Shutdown(args) => commands::stop(*args).await,
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
enum Commands {
    /// Initialize and start a new enclave
    Initialize(Box<InitializeArgs>),
    /// Connect to an existing enclave's logs
    Connect(Box<ConnectArgs>),
    /// Shutdown a running enclave
    Shutdown(Box<StopArgs>),
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct InitializeArgs {
    #[arg(long, required = true)]
    certificates_bucket: String,

    #[arg(long, default_value_t = Ipv4Cidr::new(Ipv4Addr::new(10, 0, 0, 0), 24).unwrap())]
    cidr: Ipv4Cidr,

    #[arg(index = 1, default_value = "/var/lib/proven/enclave.eif")]
    eif_path: PathBuf,

    #[clap(long)]
    email: Vec<String>,

    #[arg(long, default_value_t = 4)]
    enclave_cid: u32,

    #[arg(long, default_value_t = 10)]
    enclave_cpus: u8,

    #[arg(long, default_value_t = Ipv4Addr::new(10, 0, 0, 2))]
    enclave_ip: Ipv4Addr,

    #[arg(long, default_value_t = 25000)]
    enclave_memory: u32,

    #[clap(long, required = true)]
    fqdn: String,

    #[arg(long, default_value_t = Ipv4Addr::new(10, 0, 0, 1))]
    host_ip: Ipv4Addr,

    #[arg(long, default_value_t = 443)]
    https_port: u16,

    #[clap(long, required = true)]
    kms_key_id: String,

    #[arg(long, default_value_t = 1026)]
    log_port: u32,

    #[arg(long, default_value_t = 100)]
    max_runtime_workers: u32,

    #[arg(long, default_value_t = 4222)]
    nats_port: u16,

    #[clap(long, required = true)]
    nfs_mount_point: String,

    #[arg(long, default_value_t = format!("ens5"))]
    outbound_device: String,

    #[arg(long, default_value_t = 1025)]
    proxy_port: u32,

    #[arg(long, default_value_t = false)]
    skip_fsck: bool,

    #[arg(long, default_value_t = false)]
    skip_speedtest: bool,

    #[arg(long, default_value_t = false)]
    skip_vacuum: bool,

    #[arg(long, default_value_t = false)]
    stokenet: bool,

    #[arg(long, default_value_t = format!("tun0"))]
    tun_device: String,
}

#[derive(Parser, Debug)]
struct ConnectArgs {
    #[arg(long, default_value_t = 1026)]
    log_port: u32,
}

#[derive(Parser, Debug)]
struct StopArgs {
    #[arg(long, default_value_t = 4)]
    enclave_cid: u32,

    #[arg(long)]
    force: bool,
}
