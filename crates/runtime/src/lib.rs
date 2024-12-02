//! Manages a pool of V8 isolates for running proven application code.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod extensions;
mod options;
mod options_parser;
mod permissions;
mod pool;
mod runtime;
mod schema;
mod vendor_replacements;
mod worker;

pub use error::*;
pub use pool::*;
pub use runtime::*;
pub use worker::*;

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Request for a runtime execution.
pub struct ExecutionRequest {
    /// The accounts of the executing user.
    pub accounts: Option<Vec<String>>,

    /// The arguments to the handler.
    pub args: Vec<Value>,

    /// The address of the dApp definition.
    pub dapp_definition_address: String,

    /// The identity of the executing user.
    pub identity: Option<String>,
}

/// Logs from a runtime execution.
#[derive(Debug, Deserialize, Serialize)]
pub struct ExecutionLogs {
    /// The log arguments - should always be an array.
    pub args: Value,

    /// The log level.
    pub level: String,
}

/// Result of a runtime execution.
#[derive(Debug, Deserialize, Serialize)]
pub struct ExecutionResult {
    /// The duration of the execution.
    pub duration: Duration,

    /// The logs from the execution.
    pub logs: Vec<ExecutionLogs>,

    /// The output of the execution.
    pub output: Value,
}
