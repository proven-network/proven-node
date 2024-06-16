mod error;
mod net;

use error::Result;
use net::{bring_up_loopback, setup_default_gateway, write_dns_resolv};

use proven_vsock_cac::{listen_for_commands, Command, InitializeArgs};
use proven_vsock_proxy::Proxy;
use tokio_util::sync::CancellationToken;
use tokio_vsock::{VsockAddr, VsockStream};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    listen_for_commands(
        VsockAddr::new(4, 1024), // Control port is always at 1024
        |command| async {
            match command {
                Command::Initialize(args) => {
                    let _ = initialize(args).await;
                }
                Command::Shutdown => {
                    std::process::exit(0);
                }
            }
        },
    )
    .await?;

    Ok(())
}

async fn initialize(args: InitializeArgs) -> Result<()> {
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

    tokio::select! {
        _ = proxy_handle => {}
    }

    Ok(())
}
