//! Manages a pool of V8 isolates for running proven application code.
#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::result_large_err)]

mod error;
mod execution_request;
mod execution_result;
mod extensions;
mod file_system;
mod handler_specifier;
mod import_replacements;
mod manager;
mod module_loader;
mod options;
mod options_parser;
mod permissions;
mod pool;
mod rpc_endpoints;
mod runtime;
mod schema;
mod util;
mod worker;

pub use error::*;
pub use execution_request::ExecutionRequest;
pub use execution_result::{ExecutionLogs, ExecutionResult};
pub use file_system::StoredEntry;
pub use handler_specifier::HandlerSpecifier;
pub use manager::*;
pub use module_loader::ModuleLoader;
pub use options::*;
pub use pool::*;
pub use rpc_endpoints::RpcEndpoints;
pub use runtime::*;
pub use worker::*;
