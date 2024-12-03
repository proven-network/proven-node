use serde::{Deserialize, Serialize};

/// A request to shut down the server.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ShutdownResponse {
    /// Whether the server successfully shut down.
    pub success: bool,
}
