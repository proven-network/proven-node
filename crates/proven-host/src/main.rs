mod error;
mod net;

use error::Result;
use net::{configure_nat, configure_route, configure_tcp_forwarding};

use std::convert::Infallible;
use std::net::{Ipv4Addr, SocketAddr};

use bytes::Bytes;
use cidr::Ipv4Cidr;
use clap::Parser;
use http_body_util::Full;
use hyper::header;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioIo, TokioTimer};
use proven_vsock_cac::{send_command, Command};
use proven_vsock_proxy::Proxy;
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

    #[arg(long, default_value_t = Ipv4Addr::new(10, 0, 0, 2))]
    enclave_ip: Ipv4Addr,

    #[arg(long, default_value_t = Ipv4Addr::new(10, 0, 0, 1))]
    ip: Ipv4Addr,

    #[arg(long, default_value_t = 1026)]
    log_port: u32,

    #[arg(long, default_value_t = format!("ens5"))]
    outbound_device: String,

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

    let tracker = TaskTracker::new();

    tracker.spawn(start_proxy_server(cancellation_token.clone()));
    tracker.spawn(start_http_server(cancellation_token.clone()));

    tracker.close();

    match tokio::signal::ctrl_c().await {
        Ok(()) => {}
        Err(err) => {
            error!("unable to listen for shutdown signal: {}", err);
        }
    }

    info!("shutting down...");

    cancellation_token.cancel();

    tracker.wait().await;

    Ok(())
}

async fn start_proxy_server(cancellation_token: CancellationToken) -> Result<()> {
    let args = Args::parse();

    let mut vsock = VsockListener::bind(VsockAddr::new(3, args.proxy_port)).unwrap();

    let proxy_handler = Proxy::new(args.ip, args.enclave_ip, args.cidr, args.tun_device.clone())
        .start(async {
            configure_nat(&args.outbound_device, args.cidr).await?;
            configure_route(&args.tun_device, args.cidr, args.enclave_ip).await?;
            configure_tcp_forwarding(args.ip, args.enclave_ip, &args.outbound_device).await?;

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
