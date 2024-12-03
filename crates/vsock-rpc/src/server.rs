use crate::error::{Error, Result};

use std::future::Future;
use std::pin::Pin;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_vsock::{VsockAddr, VsockListener, VsockStream};

use crate::common::{
    AddPeerRequest, AddPeerResponse, InitializeRequest, InitializeResponse, Request, Response,
    ShutdownResponse,
};

type AcknowledgerFut = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type AcknowledgerFn<Res> = Box<dyn FnOnce(Res) -> AcknowledgerFut + Send>;
type InitializeAcknowledger = AcknowledgerFn<InitializeResponse>;
type AddPeerAcknowledger = AcknowledgerFn<AddPeerResponse>;
type ShutdownAcknowledger = AcknowledgerFn<ShutdownResponse>;

/// RPC calls that can be made to the server.
pub enum RpcCall {
    /// Initializes the enclave (after booting).
    Initialize(InitializeRequest, InitializeAcknowledger),

    /// Tell enclave about a new peer.
    AddPeer(AddPeerRequest, AddPeerAcknowledger),

    /// Tell enclave to shut down.
    Shutdown(ShutdownAcknowledger),
}

/// Listens for incoming RPC calls on vsock.
pub struct RpcServer {
    vsock_addr: VsockAddr,
}

impl RpcServer {
    /// Creates a new `RpcServer` that listens on the given vsock address.
    #[must_use]
    pub const fn new(vsock_addr: VsockAddr) -> Self {
        Self { vsock_addr }
    }

    /// Accepts an incoming RPC call.
    ///
    /// # Errors
    ///
    /// This function will return an error if there is an issue with binding the listener,
    /// accepting a connection, reading from the stream, or deserializing the request.
    pub async fn accept(&self) -> Result<RpcCall> {
        let mut listener = VsockListener::bind(self.vsock_addr)
            .map_err(|e| Error::Io("failed to bind vsock listener to address", e))?;
        let (mut stream, _) = listener
            .accept()
            .await
            .map_err(|e| Error::Io("failed to accept connection on vsock listener", e))?;

        let length = stream
            .read_u32()
            .await
            .map_err(|e| Error::Io("failed to read length", e))?;
        let mut buffer = vec![0u8; length as usize];
        stream
            .read_exact(&mut buffer)
            .await
            .map_err(|e| Error::Io("failed to read body from stream", e))?;

        let command: Request = ciborium::de::from_reader(&buffer[..])?;

        Ok(match command {
            Request::Initialize(args) => RpcCall::Initialize(
                args,
                Box::new(move |response| Box::pin(send_response(stream, response))),
            ),
            Request::AddPeer(args) => RpcCall::AddPeer(
                args,
                Box::new(move |response| Box::pin(send_response(stream, response))),
            ),
            Request::Shutdown => RpcCall::Shutdown(Box::new(move |response| {
                Box::pin(send_response(stream, response))
            })),
        })
    }
}

/// Sends a response to the client.
async fn send_response<Res: Into<Response> + Send>(
    mut stream: VsockStream,
    response: Res,
) -> Result<()> {
    let response_enum: Response = response.into();
    let mut encoded = Vec::new();
    ciborium::ser::into_writer(&response_enum, &mut encoded)?;
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

    Ok(())
}
