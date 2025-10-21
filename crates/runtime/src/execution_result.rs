use crate::error::{Error, Result};

use std::time::Duration;

use deno_core::error::JsError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
                Box::new(error.clone()),
            ))),
        }
    }
}
