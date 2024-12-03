mod add_peer;
mod initialize;
mod shutdown;

pub use add_peer::*;
pub use initialize::*;
pub use shutdown::*;

use derive_more::From;
use serde::{Deserialize, Serialize};

/// A request to the server.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Request {
    /// Add a new peer to the server.
    AddPeer(AddPeerRequest),

    /// Initialize the server.
    Initialize(InitializeRequest),

    /// Tell the server to shut down.
    Shutdown,
}

/// The response to a request.
#[derive(Clone, Debug, Deserialize, Eq, From, PartialEq, Serialize)]
pub enum Response {
    /// Unhandled by running enclave.
    Unhandled,

    /// Response to an add peer request.
    #[from]
    AddPeer(AddPeerResponse),

    /// Response to an initialize request.
    #[from]
    Initialize(InitializeResponse),

    /// Response to a shutdown request.
    #[from]
    Shutdown(ShutdownResponse),
}
