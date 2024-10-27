mod add_peer;
mod initialize;
mod shutdown;

pub use add_peer::*;
pub use initialize::*;
pub use shutdown::*;

use derive_more::From;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Request {
    AddPeer(AddPeerRequest),
    Initialize(InitializeRequest),
    Shutdown,
}

#[derive(Clone, Debug, Deserialize, Eq, From, PartialEq, Serialize)]
pub enum Response {
    Unhandled,
    #[from]
    AddPeer(AddPeerResponse),
    #[from]
    Initialize(InitializeResponse),
    #[from]
    Shutdown(ShutdownResponse),
}
