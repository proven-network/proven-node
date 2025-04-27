//! Error types for the isolated-bitcoin-core crate.

use std::io;

use proven_bootable::BootableError;
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

    /// IO error.
    #[error("io error ({0}): {1}")]
    Io(&'static str, #[source] io::Error),

    /// Error from the isolation crate.
    #[error("isolation error: {0}")]
    Isolation(#[from] proven_isolation::Error),

    /// Failed to start bitcoind
    #[error("failed to start bitcoind: {0}")]
    StartBitcoind(String),

    /// RPC call failed
    #[error("rpc call failed: {0}")]
    RpcCall(String),
}

impl BootableError for Error {}
