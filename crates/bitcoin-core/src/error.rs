//! Error types for the isolated-bitcoin-core crate.

use std::io;

use thiserror::Error;

/// Error type for the isolated-bitcoin-core crate.
#[derive(Debug, Error)]
pub enum Error
where
    Self: Send + Sync,
{
    /// Already started.
    #[error("already started")]
    AlreadyStarted,

    /// Binary not found.
    #[error("bitcoind binary not found")]
    BinaryNotFound,

    /// IO error.
    #[error("io error ({0}): {1}")]
    Io(&'static str, #[source] io::Error),

    /// Error from the isolation crate.
    #[error("isolation error: {0}")]
    Isolation(#[from] proven_isolation::Error),

    /// Process not started.
    #[error("process not started")]
    NotStarted,

    /// Failed to start bitcoind
    #[error("failed to start bitcoind: {0}")]
    StartBitcoind(String),

    /// RPC call failed
    #[error("rpc call failed: {0}")]
    RpcCall(String),

    /// Failed to parse URL
    #[error("failed to parse URL: {0}")]
    UrlParse(#[from] url::ParseError),
}
