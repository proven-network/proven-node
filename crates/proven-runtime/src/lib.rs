mod error;
mod extensions;
mod pool;
mod runtime;
mod worker;

pub use error::*;
pub use pool::*;
pub use runtime::*;
pub use worker::*;

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub struct ExecutionRequest {
    pub accounts: Option<Vec<String>>,
    pub args: Vec<Value>,
    pub dapp_definition_address: String,
    pub identity: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExecutionResult {
    pub output: Value,
    pub duration: Duration,
    pub logs: Vec<String>,
}
