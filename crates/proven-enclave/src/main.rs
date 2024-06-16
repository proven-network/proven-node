mod error;
mod net;

use error::Result;
use net::{bring_up_loopback, setup_default_gateway, write_dns_resolv};

use std::net::{Ipv4Addr, SocketAddrV4};

use proven_imds::{IdentityDocument, Imds};
use proven_nats_server::NatsServer;
use proven_vsock_proxy::Proxy;
use proven_vsock_rpc::{listen_for_commands, Command, InitializeArgs};
use proven_vsock_tracing::configure_logging_to_vsock;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_vsock::{VsockAddr, VsockStream, VMADDR_CID_ANY};
use tracing::info;
use tracing_panic::panic_hook;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let shutdown_token = CancellationToken::new();
    let task_tracker = TaskTracker::new();

    listen_for_commands(
        VsockAddr::new(VMADDR_CID_ANY, 1024), // Control port is always at 1024
        move |command| {
            let shutdown_token = shutdown_token.clone();
            let task_tracker = task_tracker.clone();

            async move {
                match command {
                    Command::Initialize(args) => {
                        if task_tracker.is_closed() {
                            return;
                        }

                        task_tracker.spawn(initialize(args, shutdown_token));

                        task_tracker.close();
                    }
                    Command::Shutdown => {
                        shutdown_token.cancel();

                        task_tracker.wait().await;
                    }
                }
            }
        },
    )
    .await?;

    Ok(())
}

async fn initialize(args: InitializeArgs, shutdown_token: CancellationToken) -> Result<()> {
    std::panic::set_hook(Box::new(panic_hook));
    configure_logging_to_vsock(VsockAddr::new(3, args.log_port)).await?;

    write_dns_resolv(args.dns_resolv)?;
    bring_up_loopback().await?;

    let vsock_stream = VsockStream::connect(VsockAddr::new(3, args.proxy_port))
        .await
        .unwrap();

    let proxy = Proxy::new(
        args.enclave_ip,
        args.host_ip,
        args.cidr,
        args.tun_device.clone(),
    );
    let connection_handler = proxy
        .start(async {
            setup_default_gateway(args.tun_device.as_str(), args.host_ip, args.cidr).await?;

            Ok(())
        })
        .await
        .unwrap();

    let proxy_ct = CancellationToken::new();
    let proxy_handle = tokio::spawn(async move {
        connection_handler
            .proxy(vsock_stream, proxy_ct.clone())
            .await
    });

    let identity = fetch_imds_identity().await?;
    info!("identity: {:?}", identity);
    let server_name = identity.instance_id;

    let nats_server = NatsServer::new(
        server_name,
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, args.nats_port),
    );
    let nats_server_handle = nats_server.start().await?;

    tokio::select! {
        _ = shutdown_token.cancelled() => {
            info!("shutdown command received");
        }
        _ = proxy_handle => {
            info!("proxy handler exited");
        }
        _ = nats_server_handle => {
            info!("nats server exited");
        }
    }

    info!("shutting down...");
    nats_server.shutdown().await;
    info!("shutdown complete. goodbye.");

    Ok(())
}

async fn fetch_imds_identity() -> Result<IdentityDocument> {
    let imds = Imds::new().await?;
    let identity = imds.get_verified_identity_document().await?;

    Ok(identity)
}
