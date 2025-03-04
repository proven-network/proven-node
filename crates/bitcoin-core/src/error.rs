//! Error types for the Bitcoin Core integration.

use std::io;
use std::process::ExitStatus;

/// Result type for Bitcoin Core operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur when working with Bitcoin Core.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Failed to start the Bitcoin Core daemon.
    #[error("failed to start bitcoind: {0}")]
    StartBitcoind(String),

    /// Failed to stop the Bitcoin Core daemon.
    #[error("failed to stop bitcoind: {0}")]
    StopBitcoind(String),

    /// Failed to create the Bitcoin Core data directory.
    #[error("failed to create data directory: {0}")]
    CreateDataDir(String),

    /// Bitcoin Core exited with a non-zero status.
    #[error("bitcoind exited with status: {0}")]
    BitcoindExited(ExitStatus),

    /// Bitcoin Core exited unexpectedly.
    #[error("bitcoind exited unexpectedly")]
    BitcoindExitedUnexpectedly,

    /// IO error.
    #[error("io error: {0} - {1}")]
    Io(&'static str, #[source] io::Error),

    /// Bitcoin Core RPC error.
    #[error("bitcoind RPC error: {0}")]
    RpcError(String),

    /// Bitcoin Core is not ready.
    #[error("bitcoind is not ready")]
    NotReady,
}
