use crate::error::Result;

use std::future::Future;
use std::pin::Pin;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_vsock::{VsockAddr, VsockListener, VsockStream};

use crate::common::*;

type AcknowledgerFut = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type AcknowledgerFn<Res> = Box<dyn FnOnce(Res) -> AcknowledgerFut + Send>;
type InitializeAcknowledger = AcknowledgerFn<InitializeResponse>;
type AddPeerAcknowledger = AcknowledgerFn<AddPeerResponse>;
type ShutdownAcknowledger = AcknowledgerFn<ShutdownResponse>;

pub enum RpcCall {
    Initialize(InitializeRequest, InitializeAcknowledger),
    AddPeer(AddPeerRequest, AddPeerAcknowledger),
    Shutdown(ShutdownAcknowledger),
}

pub struct RpcServer {
    vsock_addr: VsockAddr,
}

impl RpcServer {
    pub fn new(vsock_addr: VsockAddr) -> Self {
        Self { vsock_addr }
    }

    pub async fn accept(&self) -> Result<RpcCall> {
        let mut listener = VsockListener::bind(self.vsock_addr)?;
        let (mut stream, _) = listener.accept().await?;

        let length = stream.read_u32().await?;
        let mut buffer = vec![0u8; length as usize];
        stream.read_exact(&mut buffer).await?;

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

async fn send_response<Res: Into<Response>>(mut stream: VsockStream, response: Res) -> Result<()> {
    let response_enum: Response = response.into();
    let mut encoded = Vec::new();
    ciborium::ser::into_writer(&response_enum, &mut encoded)?;
    let length_prefix = (encoded.len() as u32).to_be_bytes();

    stream.write_all(&length_prefix).await?;
    stream.write_all(&encoded).await?;

    Ok(())
}
