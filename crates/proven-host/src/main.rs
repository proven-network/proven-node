mod error;
mod net;

use error::Result;
use net::{configure_nat, configure_route, configure_tcp_forwarding};
use tokio::process::Child;

use std::convert::Infallible;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use bytes::Bytes;
use cidr::Ipv4Cidr;
use clap::Parser;
use http_body_util::Full;
use hyper::header;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioIo, TokioTimer};
use proven_vsock_proxy::Proxy;
use proven_vsock_rpc::{send_command, Command, InitializeArgs};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_vsock::{VsockAddr, VsockListener};
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value_t = Ipv4Cidr::new(Ipv4Addr::new(10, 0, 0, 0), 24).unwrap())]
    cidr: Ipv4Cidr,

    #[arg(index = 1)]
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
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    tokio::spawn(async {
        let args = Args::parse();

        let mut vsock = VsockListener::bind(VsockAddr::new(3, args.log_port)).unwrap();
        match vsock.accept().await {
            Ok((mut stream, addr)) => {
                info!("accepted log connection from {}", addr);

                match tokio::io::copy(&mut stream, &mut tokio::io::stdout()).await {
                    Ok(_) => {}
                    Err(err) => {
                        error!("error copying from VsockStream to stdout: {:?}", err);
                    }
                }
            }
            Err(err) => {
                error!("error accepting connection: {:?}", err);
            }
        }
    });

    let cancellation_token = CancellationToken::new();

    let mut enclave = start_enclave().await?;

    let tracker = TaskTracker::new();
    tracker.spawn(start_proxy_server(cancellation_token.clone()));
    tracker.spawn(start_http_server(cancellation_token.clone()));
    tracker.close();

    // sleep for a bit to allow everything to start
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    initialize_enclave().await?;

    info!("enclave initialized successfully");

    match tokio::signal::ctrl_c().await {
        Ok(()) => {}
        Err(err) => {
            error!("unable to listen for shutdown signal: {}", err);
        }
    }

    info!("shutting down...");

    cancellation_token.cancel();

    enclave.wait().await?;
    tracker.wait().await;

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

    let dns_resolv = std::fs::read_to_string("/etc/resolv.conf").unwrap();

    let initialize_args = InitializeArgs {
        cidr: args.cidr,
        dns_resolv,
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

async fn start_proxy_server(cancellation_token: CancellationToken) -> Result<()> {
    let args = Args::parse();

    let mut vsock = VsockListener::bind(VsockAddr::new(3, args.proxy_port)).unwrap();

    let proxy_handler = Proxy::new(
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
                _ = cancellation_token.cancelled() => {
                    info!("shutting down proxy server...");
                    break;
                }
                result = vsock.accept() => {
                    match result {
                         Ok((vsock_stream, remote_addr)) => {
                        info!("accepted connection from {:?}", remote_addr);

                        let control_addr = VsockAddr::new(remote_addr.cid(), 1024); // Control port is always 1024

                        tokio::select! {
                            _ = cancellation_token.cancelled() => {
                                info!("sending shutdown signal to enclave...");
                                let _ = send_command(control_addr, Command::Shutdown).await;
                            }
                            _ = proxy_handler.proxy(vsock_stream, cancellation_token.clone()) => {}

                        }
                    },
                    Err(err) => {
                        error!("error accepting connection: {:?}", err);
                    }
                }
            }
        }
    }

    Ok(())
}

async fn start_http_server(cancellation_token: CancellationToken) -> Result<()> {
    let addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, 80));
    let listener = TcpListener::bind(addr).await?;

    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("shutting down http server...");
                break;
            }
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => {
                        if let Err(err) = http1::Builder::new()
                            .timer(TokioTimer::new())
                            .serve_connection(TokioIo::new(stream), service_fn(redirect_to_https))
                            .await
                        {
                            error!("error serving connection: {:?}", err);
                        }
                    }
                    Err(err) => {
                        error!("error accepting connection: {:?}", err);
                    }
                }
            }
        }
    }

    Ok(())
}

async fn redirect_to_https(
    req: Request<hyper::body::Incoming>,
) -> std::result::Result<Response<Full<Bytes>>, Infallible> {
    let host = req
        .headers()
        .get(header::HOST)
        .map(|h| h.to_str().unwrap())
        .unwrap_or("weareborderline.com");
    let path = req.uri().path();
    let query = req.uri().query();
    let location = format!(
        "https://{}{}{}",
        host,
        path,
        query.map(|q| format!("?{}", q)).unwrap_or_default()
    );

    let resp = Response::builder()
        .status(301)
        .header("Location", location)
        .body(Full::new(Bytes::from("Redirecting to HTTPS...")))
        .unwrap();

    Ok(resp)
}
