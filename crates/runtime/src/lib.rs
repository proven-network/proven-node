//! Manages a pool of V8 isolates for running proven application code.
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

use std::sync::LazyLock;
use std::time::Duration;

use bytes::Bytes;
use deno_core::error::JsError;
use http::Method;
use proven_sessions::Identity;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// Initialize rustls crypto provider at load time using LazyLock
static RUSTLS_INIT: LazyLock<()> = LazyLock::new(|| {
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let result = provider.install_default();
    if let Err(e) = result {
        eprintln!("Warning: Failed to install rustls CryptoProvider: {:?}", e);
    }
});

/// Request for a runtime execution.
#[derive(Clone)]
pub enum ExecutionRequest {
    /// A request received from an HTTP endpoint.
    Http {
        /// The application ID.
        application_id: String,

        /// The body of the HTTP if there was one.
        body: Option<Bytes>,

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
        /// The application ID.
        application_id: String,

        /// The body of the HTTP if there was one.
        body: Option<Bytes>,

        /// The handler specifier to execute.
        handler_specifier: HandlerSpecifier,

        /// The identity of the authenticated user.
        identities: Vec<Identity>,

        /// The HTTP method.
        method: Method,

        /// The path of the HTTP request.
        path: String,

        /// The query string of the HTTP request.
        query: Option<String>,
    },
    /// A request created to respond to an event from the Radix network.
    RadixEvent {
        /// The application ID.
        application_id: String,
        // TODO: should have Radix transaction data
        /// The handler specifier to execute.
        handler_specifier: HandlerSpecifier,
    },
    /// A request received over RPC.
    Rpc {
        /// The application ID.
        application_id: String,

        /// The arguments to the handler.
        args: Vec<Value>,

        /// The handler specifier to execute.
        handler_specifier: HandlerSpecifier,
    },
    /// A request received over RPC with authenticated user context.
    RpcWithUserContext {
        /// The application ID.
        application_id: String,

        /// The arguments to the handler.
        args: Vec<Value>,

        /// The handler specifier to execute.
        handler_specifier: HandlerSpecifier,

        /// The identity of the authenticated user.
        identities: Vec<Identity>,
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
pub enum ExecutionResult {
    /// The result of a successful execution
    Ok {
        /// The duration of the execution.
        duration: Duration,

        /// The logs from the execution.
        logs: Vec<ExecutionLogs>,

        /// The output of the execution.
        output: Value,

        /// `JSONPath` locations of any `Uint8Array` elements in the output.
        paths_to_uint8_arrays: Vec<String>,
    },

    /// Encountered a runtime error during execution.
    Error {
        /// The duration of the execution.
        duration: Duration,

        /// The logs from the execution.
        logs: Vec<ExecutionLogs>,

        /// The error message.
        error: JsError,
    },
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
        match self {
            Self::Ok { output, .. } => serde_json::from_value(output.clone()).map_err(Into::into),
            Self::Error { error, .. } => Err(Error::RuntimeError(rustyscript::Error::JsError(
                error.clone(),
            ))),
        }
    }
}
