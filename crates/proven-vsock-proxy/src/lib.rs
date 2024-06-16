mod error;

pub use crate::error::{Error, Result};

use cidr::Ipv4Cidr;
use std::sync::Arc;
use std::{future::Future, net::Ipv4Addr};
use tokio::io::{split, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio_tun::{Tun, TunBuilder};
use tokio_util::sync::CancellationToken;
use tokio_vsock::VsockStream;
use tracing::{error, info};

const FRAME_LEN: usize = 0xffff;
const FRAME_SIZE_LEN: usize = 2;

pub struct Proxy {
    pub dest_addr: Ipv4Addr,
    pub ip_addr: Ipv4Addr,
    pub cidr: Ipv4Cidr,
    pub tun_interface_name: String,
}

impl Proxy {
    pub fn new(
        ip_addr: Ipv4Addr,
        dest_addr: Ipv4Addr,
        cidr: Ipv4Cidr,
        tun_interface_name: String,
    ) -> Proxy {
        Proxy {
            dest_addr,
            ip_addr,
            cidr,
            tun_interface_name,
        }
    }

    pub async fn start(
        &self,
        post_dev_create_fn: impl Future<
            Output = std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>,
        >,
    ) -> Result<ConnectionHandler> {
        let tun = Arc::new(
            TunBuilder::new()
                .name(&self.tun_interface_name)
                .tap(false)
                .packet_info(false)
                .mtu(FRAME_LEN as i32)
                .address(self.ip_addr)
                .destination(self.dest_addr)
                .netmask(self.cidr.mask())
                .up()
                .try_build()?,
        );

        tokio::process::Command::new("tc")
            .args([
                "qdisc",
                "replace",
                "dev",
                &self.tun_interface_name,
                "root",
                "pfifo_fast",
            ])
            .output()
            .await?;

        info!("created tun interface");

        post_dev_create_fn.await.map_err(Error::Callback)?;

        Ok(ConnectionHandler { tun })
    }
}

pub struct ConnectionHandler {
    tun: Arc<Tun>,
}

impl ConnectionHandler {
    pub async fn proxy(
        &self,
        vsock_stream: VsockStream,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        let (vsock_read, vsock_write) = split(vsock_stream);

        let tun_read = Arc::clone(&self.tun);
        let tun_write = Arc::clone(&self.tun);

        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("proxy connection cancelled");

                return Ok(());
            }
            e = tokio::spawn(tun_to_vsock(tun_read, vsock_write)) => {
                error!("tun_to_vsock error");

                e?
            }
            e = tokio::spawn(vsock_to_tun(vsock_read, tun_write)) => {
                error!("vsock_to_tun error");

                e?
            }
        }?;

        info!("proxy connection closed");

        Ok(())
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
