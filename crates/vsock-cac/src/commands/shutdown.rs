//! Shutdown command implementation.

use serde::{Deserialize, Serialize};

/// Request to shutdown the enclave.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ShutdownRequest {
    /// Grace period in seconds before forceful shutdown.
    pub grace_period_secs: Option<u64>,
}

/// Response to shutdown request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ShutdownResponse {
    /// Whether the shutdown was initiated successfully.
    pub success: bool,
    /// Optional message about the shutdown status.
    pub message: Option<String>,
}
