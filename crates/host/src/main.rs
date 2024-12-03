//! Binary to run on the host machine to manage enclave lifecycle.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]

mod error;
mod net;
mod systemctl;

use error::{Error, Result};
use net::{configure_nat, configure_port_forwarding, configure_route};

use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::http::Uri;
use axum::response::Redirect;
use axum::routing::any;
use axum::Router;
use cidr::Ipv4Cidr;
use clap::{arg, command, Parser};
use nix::unistd::Uid;
use proven_http::HttpServer;
use proven_http_insecure::InsecureHttpServer;
use proven_vsock_proxy::Proxy;
use proven_vsock_rpc::{InitializeRequest, RpcClient};
use proven_vsock_tracing::host::TracingService;
use tokio::process::Child;
use tokio_vsock::{VsockAddr, VsockListener, VMADDR_CID_ANY};
use tracing::{error, info};

static ALLOCATOR_CONFIG_TEMPLATE: &str = include_str!("../templates/allocator.yaml");

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

#[tokio::main(worker_threads = 12)]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Initialize(args) => initialize(*args).await,
        Commands::Connect(args) => connect(*args).await,
    }
}

async fn initialize(args: InitializeArgs) -> Result<()> {
    if !Uid::effective().is_root() {
        return Err(Error::NotRoot);
    }

    if !Path::new(&args.eif_path).exists() {
        return Err(Error::EifDoesNotExist(args.eif_path.clone()));
    }

    let tracing_service = TracingService::new();
    let tracing_handle = tracing_service.start(args.log_port)?;

    stop_existing_enclaves().await?;

    info!("allocating enclave resources...");
    allocate_enclave_resources(args.enclave_cpus, args.enclave_memory)?;

    let _enclave = start_enclave(&args)?;

    let vsock = VsockListener::bind(VsockAddr::new(VMADDR_CID_ANY, args.proxy_port)).unwrap();

    let proxy = Arc::new(
        Proxy::new(
            args.host_ip,
            args.enclave_ip,
            args.cidr,
            args.tun_device.clone(),
        )
        .await?,
    );

    configure_nat(&args.outbound_device, args.cidr).await?;
    configure_route(&args.tun_device, args.cidr, args.enclave_ip).await?;
    configure_port_forwarding(args.host_ip, args.enclave_ip, &args.outbound_device).await?;

    let proxy_handle = proxy.clone().start_host(vsock);

    let http_server = InsecureHttpServer::new(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 80)));
    let fqdn = args.fqdn.clone();
    let http_redirector = Router::new().route(
        "/*path",
        any(move |uri: Uri| {
            let fqdn = fqdn.clone();
            async move {
                let https_uri = format!("https://{fqdn}{uri}");
                Redirect::permanent(&https_uri)
            }
        }),
    );
    let http_server_handle = http_server.start(http_redirector).await?;

    // Tasks that must be running for the host to function
    let critical_tasks = tokio::spawn(async move {
        tokio::select! {
            Err(e) = http_server_handle => {
                error!("http_server exited: {:?}", e);
            }
            () = proxy_handle => {
                error!("proxy exited");
            }
            else => {
                info!("all critical tasks exited normally");
                tracing_service.shutdown().await;
                tracing_handle.await.unwrap();
            }
        }
    });

    // sleep for a bit to allow everything to start
    tokio::time::sleep(std::time::Duration::from_secs(20)).await;
    initialize_enclave(&args).await?;

    info!("enclave initialized successfully");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("shutting down...");
            shutdown_enclave(&args).await?;
            // enclave.wait().await?; // TODO: this doesn't do anything - should poll active enclaves to check instead
            http_server.shutdown().await;
            proxy.shutdown().await;
        }
        _ = critical_tasks => {
            error!("critical task failed - exiting");
        }
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    info!("deallocating enclave resources...");
    allocate_enclave_resources(1, 0)?;
    info!("host shutdown cleanly. goodbye.");

    Ok(())
}

async fn connect(args: ConnectArgs) -> Result<()> {
    let tracing_service = TracingService::new();
    let tracing_handle = tracing_service.start(args.log_port)?;

    info!("Connected to enclave logs. Press Ctrl+C to exit.");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("shutting down...");
            tracing_service.shutdown().await;
            tracing_handle.await.unwrap();
        }
    }

    Ok(())
}

async fn stop_existing_enclaves() -> Result<()> {
    tokio::process::Command::new("nitro-cli")
        .arg("terminate-enclave")
        .arg("--all")
        .output()
        .await
        .map_err(|e| Error::Io("failed to stop existing enclaves", e))?;

    Ok(())
}

fn start_enclave(args: &InitializeArgs) -> Result<Child> {
    let handle = tokio::process::Command::new("nitro-cli")
        .arg("run-enclave")
        .arg("--cpu-count")
        .arg(args.enclave_cpus.to_string())
        .arg("--memory")
        .arg(args.enclave_memory.to_string())
        .arg("--enclave-cid")
        .arg(args.enclave_cid.to_string())
        .arg("--eif-path")
        .arg(args.eif_path.clone())
        .spawn()
        .map_err(|e| Error::Io("failed to start enclave", e))?;

    Ok(handle)
}

fn allocate_enclave_resources(enclave_cpus: u8, enclave_memory: u32) -> Result<()> {
    let existing_allocator_config = std::fs::read_to_string("/etc/nitro_enclaves/allocator.yaml")
        .map_err(|e| Error::Io("failed to read allocator config", e))?;
    if existing_allocator_config.contains(&format!("cpu_count: {enclave_cpus}"))
        && existing_allocator_config.contains(&format!("memory_mib: {enclave_memory}"))
    {
        return Ok(());
    }

    let allocator_config = ALLOCATOR_CONFIG_TEMPLATE
        .replace("{memory_mib}", &enclave_memory.to_string())
        .replace("{cpu_count}", &enclave_cpus.to_string());

    std::fs::write("/etc/nitro_enclaves/allocator.yaml", allocator_config)
        .map_err(|e| Error::Io("failed to write allocator config", e))?;

    systemctl::restart_allocator_service()?;

    Ok(())
}

async fn initialize_enclave(args: &InitializeArgs) -> Result<()> {
    let host_dns_resolv = std::fs::read_to_string("/etc/resolv.conf").unwrap();

    let res = RpcClient::new(VsockAddr::new(args.enclave_cid, 1024))
        .initialize(InitializeRequest {
            certificates_bucket: args.certificates_bucket.clone(),
            cidr: args.cidr,
            email: args.email.clone(),
            enclave_ip: args.enclave_ip,
            fqdn: args.fqdn.clone(),
            host_dns_resolv,
            host_ip: args.host_ip,
            https_port: args.https_port,
            kms_key_id: args.kms_key_id.clone(),
            log_port: args.log_port,
            nats_port: args.nats_port,
            nfs_mount_point: args.nfs_mount_point.clone(),
            proxy_port: args.proxy_port,
            skip_fsck: args.skip_fsck,
            skip_speedtest: args.skip_speedtest,
            skip_vacuum: args.skip_vacuum,
            stokenet: args.stokenet,
            tun_device: args.tun_device.clone(),
        })
        .await;

    info!("initialize response: {:?}", res);

    Ok(())
}

async fn shutdown_enclave(args: &InitializeArgs) -> Result<()> {
    let res = RpcClient::new(VsockAddr::new(args.enclave_cid, 1024))
        .shutdown()
        .await;

    info!("shutdown response: {:?}", res);

    Ok(())
}
