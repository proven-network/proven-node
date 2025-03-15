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
use clap::{Parser, arg, command};
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[tokio::main(worker_threads = 12)]
async fn main() -> Result<()> {
    // Host-local logging
    tracing::subscriber::set_global_default(
        FmtSubscriber::builder()
            .with_max_level(Level::TRACE)
            .finish(),
    )?;

    let cli = Cli::parse();

    match cli.command {
        Commands::Connect(args) => commands::connect(*args).await,
        Commands::Start(args) => commands::start(*args).await,
        Commands::Stop(args) => commands::stop(*args).await,
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
    /// Connect to an existing enclave's logs
    Connect(Box<ConnectArgs>),

    /// Initialize and start a new enclave
    Start(Box<StartArgs>),

    /// Shutdown a running enclave
    Stop(Box<StopArgs>),
}

#[derive(Clone, Debug, Parser)]
struct ConnectArgs {
    #[arg(long, default_value_t = 4, env = "PROVEN_ENCLAVE_CID")]
    enclave_cid: u32,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Parser)]
#[command(version, about, long_about = None)]
struct StartArgs {
    #[arg(long, required = true, env = "PROVEN_CERTIFICATES_BUCKET")]
    certificates_bucket: String,

    #[arg(long, default_value_t = Ipv4Cidr::new(Ipv4Addr::new(10, 0, 0, 0), 24).unwrap(), env = "PROVEN_CIDR")]
    cidr: Ipv4Cidr,

    #[arg(
        index = 1,
        default_value = "/var/lib/proven/enclave.eif",
        env = "PROVEN_EIF_PATH"
    )]
    eif_path: PathBuf,

    #[clap(long, env = "PROVEN_EMAIL")]
    email: Vec<String>,

    #[arg(long, default_value_t = 4, env = "PROVEN_ENCLAVE_CID")]
    enclave_cid: u32,

    #[arg(long, default_value_t = 10, env = "PROVEN_ENCLAVE_CPUS")]
    enclave_cpus: u8,

    #[arg(long, default_value_t = Ipv4Addr::new(10, 0, 0, 2), env = "PROVEN_ENCLAVE_IP")]
    enclave_ip: Ipv4Addr,

    #[arg(long, default_value_t = 25000, env = "PROVEN_ENCLAVE_MEMORY")]
    enclave_memory: u32,

    #[clap(long, required = true, env = "PROVEN_FILE_SYSTEMS_BUCKET")]
    file_systems_bucket: String,

    #[clap(long, required = true, env = "PROVEN_FQDN")]
    fqdn: String,

    #[arg(long, default_value_t = Ipv4Addr::new(10, 0, 0, 1), env = "PROVEN_HOST_IP")]
    host_ip: Ipv4Addr,

    #[arg(long, default_value_t = 443, env = "PROVEN_HTTP_PORT")]
    https_port: u16,

    #[clap(long, required = true, env = "PROVEN_KMS_KEY_ID")]
    kms_key_id: String,

    #[arg(long, default_value_t = 100, env = "PROVEN_MAX_RUNTIME_WORKERS")]
    max_runtime_workers: u32,

    #[arg(long, default_value_t = 4222, env = "PROVEN_NATS_PORT")]
    nats_port: u16,

    #[clap(long, required = true, env = "PROVEN_NFS_MOUNT_POINT")]
    nfs_mount_point: String,

    #[arg(long, default_value_t = format!("ens5"), env = "PROVEN_OUTBOUND_DEVICE")]
    outbound_device: String,

    #[arg(long, default_value_t = 1025, env = "PROVEN_PROXY_PORT")]
    proxy_port: u32,

    #[arg(long, default_value_t = false, env = "PROVEN_SKIP_FSCK")]
    skip_fsck: bool,

    #[arg(long, default_value_t = false, env = "PROVEN_SKIP_SPEEDTEST")]
    skip_speedtest: bool,

    #[arg(long, default_value_t = false, env = "PROVEN_SKIP_VACUUM")]
    skip_vacuum: bool,

    #[clap(long, required = true, env = "PROVEN_SQL_SNAPSHOTS_BUCKET")]
    sql_snapshots_bucket: String,

    #[arg(long, default_value_t = false, env = "PROVEN_STOKENET")]
    stokenet: bool,

    #[arg(long, default_value_t = format!("tun0"), env = "PROVEN_TUN_DEVICE")]
    tun_device: String,
}

#[derive(Clone, Debug, Parser)]
struct StopArgs {
    #[arg(long, default_value_t = 4)]
    enclave_cid: u32,

    #[arg(long)]
    force: bool,
}
