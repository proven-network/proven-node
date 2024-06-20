mod error;
mod http;
mod net;
mod vsock_tracing;

use error::{Error, Result};
use http::HttpServer;
use net::{configure_nat, configure_route, configure_tcp_forwarding};
use vsock_tracing::TracingService;

use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use cidr::Ipv4Cidr;
use clap::Parser;
use nix::unistd::Uid;
use proven_vsock_proxy::Proxy;
use proven_vsock_rpc::{send_command, Command, InitializeArgs};
use tokio::process::Child;
use tokio_util::sync::CancellationToken;
use tokio_vsock::{VsockAddr, VsockListener};
use tracing::{error, info};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value_t = Ipv4Cidr::new(Ipv4Addr::new(10, 0, 0, 0), 24).unwrap())]
    cidr: Ipv4Cidr,

    #[arg(index = 1, default_value = "/var/lib/proven/enclave.eif")]
    eif_path: PathBuf,

    #[clap(long)]
    email: Vec<String>,

    #[arg(long, default_value_t = 4)]
    enclave_cid: u32,

    #[arg(long, default_value_t = 2)]
    enclave_cpus: u8,

    #[arg(long, default_value_t = Ipv4Addr::new(10, 0, 0, 2))]
    enclave_ip: Ipv4Addr,

    #[arg(long, default_value_t = 800)]
    enclave_memory: u32,

    #[clap(long, required = true)]
    fqdn: String,

    #[arg(long, default_value_t = Ipv4Addr::new(10, 0, 0, 1))]
    host_ip: Ipv4Addr,

    #[arg(long, default_value_t = 443)]
    https_port: u16,

    #[arg(long, default_value_t = 1026)]
    log_port: u32,

    #[arg(long, default_value_t = 4222)]
    nats_port: u16,

    #[arg(long, default_value_t = format!("ens5"))]
    outbound_device: String,

    #[arg(long, default_value_t = false)]
    production: bool,

    #[arg(long, default_value_t = 1025)]
    proxy_port: u32,

    #[arg(long, default_value_t = format!("tun0"))]
    tun_device: String,
}

#[tokio::main(worker_threads = 8)]
async fn main() -> Result<()> {
    let args = Args::parse();

    if !Uid::effective().is_root() {
        return Err(Error::NotRoot);
    }

    if !Path::new(&args.eif_path).exists() {
        return Err(Error::EifDoesNotExist(args.eif_path));
    }

    let tracing_service = TracingService::new();
    let tracing_handle = tracing_service.start(args.log_port)?;

    let mut enclave = start_enclave().await?;

    let cancellation_token = CancellationToken::new();
    let proxy_cancel_token = cancellation_token.clone();
    let proxy_handle = tokio::spawn(async move {
        let args = Args::parse();

        let mut vsock = VsockListener::bind(VsockAddr::new(3, args.proxy_port)).unwrap();
        let connection_handler = Proxy::new(
            args.host_ip,
            args.enclave_ip,
            args.cidr,
            args.tun_device.clone(),
        )
        .start(async {
            configure_nat(&args.outbound_device, args.cidr).await?;
            configure_route(&args.tun_device, args.cidr, args.enclave_ip).await?;
            configure_tcp_forwarding(args.host_ip, args.enclave_ip, &args.outbound_device).await?;

            Ok(())
        })
        .await
        .unwrap();

        loop {
            tokio::select! {
                    _ = proxy_cancel_token.cancelled() => {
                        info!("shutting down proxy server...");
                        break;
                    }
                    result = vsock.accept() => {
                        match result {
                             Ok((vsock_stream, remote_addr)) => {
                            info!("accepted connection from {:?}", remote_addr);

                            let _ = connection_handler.proxy(vsock_stream, proxy_cancel_token.clone()).await;
                        },
                        Err(err) => {
                            error!("error accepting connection: {:?}", err);
                        }
                    }
                }
            }
        }
    });

    let http_redirector = HttpServer::new(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 80)));
    let http_redirector_handle = http_redirector.start().await?;

    // Tasks that must be running for the host to function
    let critical_tasks = tokio::spawn(async move {
        tokio::select! {
            Err(e) = http_redirector_handle => {
                error!("http_redirector exited: {:?}", e);
            }
            Err(e) = proxy_handle => {
                error!("proxy exited: {:?}", e);
            }
            else => {
                info!("all critical tasks exited normally");
                tracing_service.shutdown().await;
                tracing_handle.await.unwrap();
            }
        }
    });

    // sleep for a bit to allow everything to start
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    initialize_enclave().await?;

    info!("enclave initialized successfully");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("shutting down...");
            // Shutdown enclave first
            shutdown_enclave().await?;
            enclave.wait().await?;
            // Cancel proxy and http server
            cancellation_token.cancel();
            http_redirector.shutdown().await;
        }
        _ = critical_tasks => {
            error!("critical task failed - exiting");
        }
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    info!("host shutdown cleanly. goodbye.");

    Ok(())
}

async fn start_enclave() -> Result<Child> {
    let args = Args::parse();

    tokio::process::Command::new("nitro-cli")
        .arg("terminate-enclave")
        .arg("--all")
        .output()
        .await?;

    let handle = tokio::process::Command::new("nitro-cli")
        .arg("run-enclave")
        .arg("--cpu-count")
        .arg(args.enclave_cpus.to_string())
        .arg("--memory")
        .arg(args.enclave_memory.to_string())
        .arg("--enclave-cid")
        .arg(args.enclave_cid.to_string())
        .arg("--eif-path")
        .arg(args.eif_path)
        .spawn()?;

    Ok(handle)
}

async fn initialize_enclave() -> Result<()> {
    let args = Args::parse();

    let initialize_args = InitializeArgs {
        cidr: args.cidr,
        email: args.email,
        enclave_ip: args.enclave_ip,
        fqdn: args.fqdn,
        host_ip: args.host_ip,
        https_port: args.https_port,
        log_port: args.log_port,
        nats_port: args.nats_port,
        production: args.production,
        proxy_port: args.proxy_port,
        tun_device: args.tun_device,
    };

    send_command(
        VsockAddr::new(args.enclave_cid, 1024),
        Command::Initialize(initialize_args),
    )
    .await?;

    Ok(())
}

async fn shutdown_enclave() -> Result<()> {
    let args = Args::parse();

    send_command(VsockAddr::new(args.enclave_cid, 1024), Command::Shutdown).await?;

    Ok(())
}
