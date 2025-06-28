use proven_locks::LockManagerError;
use thiserror::Error;

/// Errors that can occur when using the NATS lock manager.
#[derive(Error, Debug)]
pub enum Error {
    /// Error creating the KV store.
    #[error("NATS JetStream context error creating KV store: {0}")]
    CreateKvError(#[from] async_nats::jetstream::context::CreateKeyValueError),

    /// Error creating a lock.
    #[error("NATS KV store create operation error: {0}")]
    CreateError(#[from] async_nats::jetstream::kv::CreateError),

    /// Error getting a lock.
    #[error("NATS KV store entry operation error (get): {0}")]
    EntryError(#[from] async_nats::jetstream::kv::EntryError),

    /// Timeout error during NATS operation.
    #[error("NATS operation timed out after {attempts} attempts: {last_error}")]
    Timeout {
        /// Number of attempts made before timing out.
        attempts: usize,
        /// The last error message received.
        last_error: String,
    },

    /// Maximum retry attempts exceeded.
    #[error("Maximum retry attempts ({max_attempts}) exceeded for NATS operation: {last_error}")]
    MaxRetriesExceeded {
        /// Maximum number of attempts that were allowed.
        max_attempts: usize,
        /// The last error message received.
        last_error: String,
    },
}

impl Error {
    /// Check if this error is potentially recoverable with retry.
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        match self {
            Self::Timeout { .. } => true,
            Self::CreateKvError(e) => is_retriable_jetstream_error(&e.to_string()),
            Self::CreateError(e) => is_retriable_kv_error(&e.to_string()),
            Self::EntryError(e) => is_retriable_kv_error(&e.to_string()),
            Self::MaxRetriesExceeded { .. } => false,
        }
    }
}

fn is_retriable_jetstream_error(error_str: &str) -> bool {
    error_str.contains("timeout")
        || error_str.contains("connection")
        || error_str.contains("unavailable")
        || error_str.contains("temporary")
}

fn is_retriable_kv_error(error_str: &str) -> bool {
    error_str.contains("timeout")
        || error_str.contains("connection")
        || error_str.contains("unavailable")
        || error_str.contains("temporary")
        || error_str.contains("stream not found") // Can happen during cluster failover
}

impl LockManagerError for Error {}
