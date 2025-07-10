//! Command definitions for CAC RPC.

mod initialize;
mod shutdown;

pub use initialize::{InitializeRequest, InitializeResponse};
pub use shutdown::{ShutdownRequest, ShutdownResponse};

use proven_vsock_rpc::RpcMessage;
use serde::{Deserialize, Serialize};

/// Envelope for all CAC commands.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CacCommand {
    /// Initialize the enclave.
    Initialize(Box<InitializeRequest>),
    /// Shutdown the enclave.
    Shutdown(ShutdownRequest),
}

/// Envelope for all CAC responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CacResponse {
    /// Response to initialization.
    Initialize(InitializeResponse),
    /// Response to shutdown.
    Shutdown(ShutdownResponse),
}

impl RpcMessage for CacCommand {
    type Response = CacResponse;

    fn message_id(&self) -> &'static str {
        match self {
            Self::Initialize(_) => "cac.initialize",
            Self::Shutdown(_) => "cac.shutdown",
        }
    }
}
