//! Command definitions for CAC RPC.

mod initialize;
mod shutdown;

use bytes::Bytes;
pub use initialize::{InitializeRequest, InitializeResponse};
pub use shutdown::{ShutdownRequest, ShutdownResponse};

use proven_vsock_rpc::RpcMessage;

// Implement RpcMessage directly for InitializeRequest
impl RpcMessage for InitializeRequest {
    type Response = InitializeResponse;

    fn message_id(&self) -> &'static str {
        "cac.initialize"
    }
}

impl TryInto<Bytes> for InitializeRequest {
    type Error = bincode::Error;

    fn try_into(self) -> std::result::Result<Bytes, Self::Error> {
        let payload = bincode::serialize(&self)?;
        Ok(Bytes::from(payload))
    }
}

impl TryFrom<Bytes> for InitializeRequest {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}

impl TryInto<Bytes> for InitializeResponse {
    type Error = bincode::Error;

    fn try_into(self) -> std::result::Result<Bytes, Self::Error> {
        let payload = bincode::serialize(&self)?;
        Ok(Bytes::from(payload))
    }
}

impl TryFrom<Bytes> for InitializeResponse {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}

// Implement RpcMessage directly for ShutdownRequest
impl RpcMessage for ShutdownRequest {
    type Response = ShutdownResponse;

    fn message_id(&self) -> &'static str {
        "cac.shutdown"
    }
}

impl TryInto<Bytes> for ShutdownRequest {
    type Error = bincode::Error;

    fn try_into(self) -> std::result::Result<Bytes, Self::Error> {
        let payload = bincode::serialize(&self)?;
        Ok(Bytes::from(payload))
    }
}

impl TryFrom<Bytes> for ShutdownRequest {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}

impl TryInto<Bytes> for ShutdownResponse {
    type Error = bincode::Error;

    fn try_into(self) -> std::result::Result<Bytes, Self::Error> {
        let payload = bincode::serialize(&self)?;
        Ok(Bytes::from(payload))
    }
}

impl TryFrom<Bytes> for ShutdownResponse {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}
