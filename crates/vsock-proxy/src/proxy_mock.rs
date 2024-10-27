use std::net::Ipv4Addr;
use std::sync::Arc;

use cidr::Ipv4Cidr;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct Proxy {
    shutdown_token: CancellationToken,
}

impl Proxy {
    pub async fn new(
        _ip_addr: Ipv4Addr,
        _dest_addr: Ipv4Addr,
        _cidr: Ipv4Cidr,
        _tun_interface_name: String,
    ) -> Result<Self, ()> {
        Ok(Self {
            shutdown_token: CancellationToken::new(),
        })
    }

    pub fn start(&self, _vsock_stream: ()) -> JoinHandle<Result<(), ()>> {
        tokio::spawn(async { Ok(()) })
    }

    pub async fn start_host(self: Arc<Self>, mut _vsock: ()) {
        loop {
            tokio::select! {
                _ = self.shutdown_token.cancelled() => {
                    break;
                }
                _ = async { Ok::<_, ()>(((), ())) } => {
                  let proxy_clone = Arc::clone(&self);
                  tokio::spawn(async move {
                      let _ = proxy_clone.start(()).await;
                  });
              }
            }
        }
    }

    pub async fn shutdown(&self) {
        self.shutdown_token.cancel();
    }
}
