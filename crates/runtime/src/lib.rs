//! Manages a pool of V8 isolates for running proven application code.
#![feature(async_closure)]
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
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
mod runtime;
mod schema;
mod util;
mod worker;

pub use error::*;
pub use file_system::StoredEntry;
pub use handler_specifier::HandlerSpecifier;
pub use manager::*;
pub use module_loader::ModuleLoader;
pub use options::*;
pub use pool::*;
pub use runtime::*;
pub use worker::*;

use std::time::Duration;

use bytes::Bytes;
use http::Method;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Request for a runtime execution.
#[derive(Clone)]
pub enum ExecutionRequest {
    /// A request received from an HTTP endpoint.
    Http {
        /// The body of the HTTP if there was one.
        body: Option<Bytes>,

        /// The address of the dApp definition.
        dapp_definition_address: String,

        /// The handler specifier to execute.
        handler_specifier: HandlerSpecifier,

        /// The HTTP method.
        method: Method,

        /// The path of the HTTP request.
        path: String,

        /// The query string of the HTTP request.
        query: Option<String>,
    },
    /// A request received from an HTTP endpoint with authenticated user context.
    HttpWithUserContext {
        /// The accounts of the authenticated user.
        accounts: Vec<String>,

        /// The body of the HTTP if there was one.
        body: Option<Bytes>,

        /// The address of the dApp definition.
        dapp_definition_address: String,

        /// The handler specifier to execute.
        handler_specifier: HandlerSpecifier,

        /// The identity of the authenticated user.
        identity: String,

        /// The HTTP method.
        method: Method,

        /// The path of the HTTP request.
        path: String,

        /// The query string of the HTTP request.
        query: Option<String>,
    },
    /// A request created to respond to an event from the Radix network.
    RadixEvent {
        /// The address of the dApp definition.
        dapp_definition_address: String,
        // TODO: should have Radix transaction data
        /// The handler specifier to execute.
        handler_specifier: HandlerSpecifier,
    },
    /// A request received over RPC.
    Rpc {
        /// The arguments to the handler.
        args: Vec<Value>,

        /// The address of the dApp definition.
        dapp_definition_address: String,

        /// The handler specifier to execute.
        handler_specifier: HandlerSpecifier,
    },
    /// A request received over RPC with authenticated user context.
    RpcWithUserContext {
        /// The accounts of the executing user.
        accounts: Vec<String>,

        /// The arguments to the handler.
        args: Vec<Value>,

        /// The address of the dApp definition.
        dapp_definition_address: String,

        /// The handler specifier to execute.
        handler_specifier: HandlerSpecifier,

        /// The identity of the executing user.
        identity: String,
    },
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

    /// `JSONPath` locations of any `Uint8Array` elements in the output.
    pub paths_to_uint8_arrays: Vec<String>,
}

impl ExecutionResult {
    /// Deserializes the output into the specified type.
    ///
    /// # Errors
    ///
    /// This function will return an error if the output cannot be deserialized into the specified type.
    pub fn deserialize_output<T>(&self) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        serde_json::from_value(self.output.clone()).map_err(Into::into)
    }
}
