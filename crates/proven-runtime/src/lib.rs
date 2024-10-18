mod extensions;
mod pool;
mod runtime;
mod worker;

pub use pool::*;
pub use runtime::*;
pub use worker::*;

use std::time::Duration;

use serde_json::Value;

pub struct Context {
    pub identity: Option<String>,
    pub accounts: Option<Vec<String>>,
}

pub struct ExecutionRequest {
    pub context: Context,
    pub handler_name: String,
    pub args: Vec<Value>,
}

#[derive(Debug)]
pub struct ExecutionResult {
    pub output: Value,
    pub duration: Duration,
    pub logs: Vec<String>,
}
