use crate::common::*;
use crate::error::{Error, Result};

use std::net::Shutdown;

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
        if let Response::Initialize(response) = self.send(Request::Initialize(args)).await? {
            Ok(response)
        } else {
            Err(Error::BadResponseType)
        }
    }

    pub async fn add_peer(&self, args: AddPeerRequest) -> Result<AddPeerResponse> {
        if let Response::AddPeer(response) = self.send(Request::AddPeer(args)).await? {
            Ok(response)
        } else {
            Err(Error::BadResponseType)
        }
    }

    pub async fn shutdown(&self) -> Result<ShutdownResponse> {
        if let Response::Shutdown(response) = self.send(Request::Shutdown).await? {
            Ok(response)
        } else {
            Err(Error::BadResponseType)
        }
    }

    async fn send(&self, command: Request) -> Result<Response> {
        debug!("sending command: {:?}", command);

        let mut stream = VsockStream::connect(self.vsock_addr).await?;
        let mut encoded = Vec::new();
        ciborium::ser::into_writer(&command, &mut encoded)?;

        let length_prefix = (encoded.len() as u32).to_be_bytes();
        stream.write_all(&length_prefix).await?;
        stream.write_all(&encoded).await?;

        let length = stream.read_u32().await?;
        let mut buffer = vec![0u8; length as usize];
        stream.read_exact(&mut buffer).await?;

        stream.shutdown(Shutdown::Both)?;

        let response: Response = ciborium::de::from_reader(&buffer[..])?;

        Ok(response)
    }
}
