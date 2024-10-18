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

// What do I want?
// I want a pool of workers that can execute user modules.
// Each worker will have its own instance of the runtime.
// Each worker will load a single user module.
// The hash of the user module will be used to identify which workers can execute which requests.
// Requests to the pool should be matched to a free worker which has the requested module loaded.
// If there are no free workers with the module loaded - a new worker should be created.
// The pool should have a maximum number of workers that can be spawned.
// If a request needs to create a new worker and the pool is at max capacity - the most-idle free worker should be killed. If no workers are free to be killed - the request should be queued until one is free
// If a execution ever fails with an OOM error - the worker should be killed and removed from the pool.
// I need to be able to send requests to the pool from async contexts (using tokio for rt).
// Runtimes cannot be sent across threads because of v8, so new workers need to be spawned in a new std thread.
// Communication between pool and worker needs to happen over a channel.
