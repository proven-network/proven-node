mod error;

pub use crate::error::{Error, Result};

#[cfg(target_os = "linux")]
mod proxy;

#[cfg(target_os = "linux")]
pub use crate::proxy::Proxy;

#[cfg(not(target_os = "linux"))]
pub use stub::Proxy;

#[cfg(not(target_os = "linux"))]
mod stub {
    use super::*;
    use proven_logger::info;
    use std::net::Ipv4Addr;
    use tokio::task::JoinHandle;

    pub struct Proxy;

    impl Proxy {
        pub fn new(_tun_addr: Ipv4Addr, _tun_mask: u8, _vsock_port: u32, _is_host: bool) -> Self {
            Self
        }

        pub async fn start(&self) -> Result<JoinHandle<Result<()>>> {
            info!("vsock-proxy is not used on non-Linux platforms");
            Ok(tokio::spawn(async { Ok(()) }))
        }

        pub async fn shutdown(&self) {
            // No-op
        }
    }
}
