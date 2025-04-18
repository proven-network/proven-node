use crate::common::{InitializeRequest, InitializeResponse, Request, Response, ShutdownResponse};
use crate::error::{Error, Result};

use std::net::Shutdown;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_vsock::{VsockAddr, VsockStream};
use tracing::debug;

/// RPC client that can send commands to the server.
pub struct RpcClient {
    vsock_addr: VsockAddr,
}

impl RpcClient {
    /// Creates a new `RpcClient` that connects to the given vsock address.
    #[must_use]
    pub const fn new(vsock_addr: VsockAddr) -> Self {
        Self { vsock_addr }
    }

    /// Sends an initialize command to the server.
    ///
    /// # Errors
    ///
    /// This function will return an error if the server responds with an unexpected response type
    /// or if there is an I/O error during communication.
    pub async fn initialize(&self, args: InitializeRequest) -> Result<InitializeResponse> {
        if let Response::Initialize(response) = self.send(Request::Initialize(args)).await? {
            Ok(response)
        } else {
            Err(Error::BadResponseType)
        }
    }

    /// Sends a shutdown command to the server.
    ///
    /// # Errors
    ///
    /// This function will return an error if the server responds with an unexpected response type
    /// or if there is an I/O error during communication.
    pub async fn shutdown(&self) -> Result<ShutdownResponse> {
        if let Response::Shutdown(response) = self.send(Request::Shutdown).await? {
            Ok(response)
        } else {
            Err(Error::BadResponseType)
        }
    }

    async fn send(&self, command: Request) -> Result<Response> {
        debug!("sending command: {:?}", command);

        let mut stream = VsockStream::connect(self.vsock_addr)
            .await
            .map_err(|e| Error::Io("failed to connect to vsock server", e))?;
        let mut encoded = Vec::new();
        ciborium::ser::into_writer(&command, &mut encoded)?;

        let length = encoded.len();
        let length_u32: u32 = length
            .try_into()
            .map_err(|_| Error::ResponseTooLarge(length, u32::MAX))?;
        let length_prefix = length_u32.to_be_bytes();

        stream
            .write_all(&length_prefix)
            .await
            .map_err(|e| Error::Io("failed to write length", e))?;
        stream
            .write_all(&encoded)
            .await
            .map_err(|e| Error::Io("failed to write body", e))?;

        let length = stream
            .read_u32()
            .await
            .map_err(|e| Error::Io("failed to read length", e))?;
        let mut buffer = vec![0u8; length as usize];
        stream
            .read_exact(&mut buffer)
            .await
            .map_err(|e| Error::Io("failed to read body", e))?;

        stream
            .shutdown(Shutdown::Both)
            .map_err(|e| Error::Io("failed to shutdown stream", e))?;

        let response: Response = ciborium::de::from_reader(&buffer[..])?;

        Ok(response)
    }
}
