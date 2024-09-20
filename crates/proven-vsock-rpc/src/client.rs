use crate::common::*;
use crate::error::{Error, Result};

use std::net::Shutdown;

use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_vsock::{VsockAddr, VsockStream};
use tracing::debug;

pub struct RpcClient {
    vsock_addr: VsockAddr,
}

impl RpcClient {
    pub fn new(vsock_addr: VsockAddr) -> Self {
        Self { vsock_addr }
    }

    pub async fn initialize(&self, args: InitializeRequest) -> Result<InitializeResponse> {
        self.send(Request::Initialize(args)).await
    }

    pub async fn add_peer(&self, args: AddPeerRequest) -> Result<AddPeerResponse> {
        self.send(Request::AddPeer(args)).await
    }

    pub async fn shutdown(&self) -> Result<ShutdownResponse> {
        self.send(Request::Shutdown).await
    }

    async fn send<Res: Into<Response> + DeserializeOwned>(&self, command: Request) -> Result<Res> {
        debug!("sending command: {:?}", command);

        let mut stream = VsockStream::connect(self.vsock_addr).await?;
        let encoded = serde_cbor::to_vec(&command)?;
        let length_prefix = (encoded.len() as u32).to_be_bytes();
        stream.write_all(&length_prefix).await?;
        stream.write_all(&encoded).await?;

        let length = stream.read_u32().await?;
        let mut buffer = vec![0u8; length as usize];
        stream.read_exact(&mut buffer).await?;

        stream.shutdown(Shutdown::Both)?;

        let response: Res = serde_cbor::from_slice(&buffer).map_err(|_| Error::BadResponseType)?;

        Ok(response)
    }
}
