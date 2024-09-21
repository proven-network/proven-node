mod error;

pub use crate::error::{Error, Result};

use std::net::Ipv4Addr;
use std::sync::Arc;

use cidr::Ipv4Cidr;
use tokio::io::{split, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::task::JoinHandle;
use tokio_tun::{Tun, TunBuilder};
use tokio_util::sync::CancellationToken;
use tokio_vsock::{VsockListener, VsockStream};
use tracing::{error, info};

const FRAME_LEN: usize = 0xffff;
const FRAME_SIZE_LEN: usize = 2;

pub struct Proxy {
    tun: Arc<Tun>,
    shutdown_token: CancellationToken,
}

impl Proxy {
    pub async fn new(
        ip_addr: Ipv4Addr,
        dest_addr: Ipv4Addr,
        cidr: Ipv4Cidr,
        tun_interface_name: String,
    ) -> Result<Self> {
        let tun = Arc::new(
            TunBuilder::new()
                .name(&tun_interface_name)
                .mtu(FRAME_LEN as i32)
                .address(ip_addr)
                .destination(dest_addr)
                .netmask(cidr.mask())
                .up()
                .try_build()?,
        );

        tokio::process::Command::new("tc")
            .args([
                "qdisc",
                "replace",
                "dev",
                &tun_interface_name,
                "root",
                "pfifo_fast",
            ])
            .output()
            .await?;

        info!("created tun interface");

        Ok(Self {
            tun,
            shutdown_token: CancellationToken::new(),
        })
    }

    pub fn start(&self, vsock_stream: VsockStream) -> JoinHandle<Result<()>> {
        let (vsock_read, vsock_write) = split(vsock_stream);

        let tun_read = Arc::clone(&self.tun);
        let tun_write = Arc::clone(&self.tun);

        let shutdown_token = self.shutdown_token.clone();

        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    return Ok(());
                }
                e = tokio::spawn(tun_to_vsock(tun_read, vsock_write)) => {
                    error!("tun_to_vsock error: {:?}", e);

                    e?
                }
                e = tokio::spawn(vsock_to_tun(vsock_read, tun_write)) => {
                    error!("vsock_to_tun error: {:?}", e);

                    e?
                }
            }?;

            Ok(())
        })
    }

    pub async fn start_host(self: Arc<Self>, mut vsock: VsockListener) {
        loop {
            tokio::select! {
                _ = self.shutdown_token.cancelled() => {
                    break;
                }
                Ok((vsock_stream, remote_addr)) = vsock.accept() => {
                    info!("accepted vsock connection from {}", remote_addr);
                    let proxy_clone = Arc::clone(&self);
                    tokio::spawn(async move {
                        let _ = proxy_clone.start(vsock_stream).await;
                    });
                }
            }
        }
    }

    pub async fn shutdown(&self) {
        info!("proxy shutting down...");

        self.shutdown_token.cancel();

        info!("proxy shutdown");
    }
}

async fn tun_to_vsock(tun: Arc<Tun>, mut vsock: WriteHalf<VsockStream>) -> Result<()> {
    info!("listening for packets on tun interface");

    let mut buf = [0; FRAME_LEN + FRAME_SIZE_LEN];

    loop {
        let n = tun.recv(&mut buf[FRAME_SIZE_LEN..]).await?;

        if n == 0 {
            continue;
        }

        buf[..FRAME_SIZE_LEN].copy_from_slice(&(n.to_le() as u16).to_le_bytes());

        vsock.write_all(&buf[..n + FRAME_SIZE_LEN]).await?
    }
}

async fn vsock_to_tun(mut vsock: ReadHalf<VsockStream>, tun: Arc<Tun>) -> Result<()> {
    info!("listening for packets on vsock interface");

    let mut buf = [0; FRAME_LEN];

    loop {
        let n = vsock.read_u16().await?.to_be() as usize;

        vsock.read_exact(&mut buf[..n]).await?;

        tun.send_all(&buf[..n]).await?;
    }
}
