//! Error types for the nats-server crate.

use std::io;
use std::process::ExitStatus;

use thiserror::Error;

/// Error type for the nats-server crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Already started.
    #[error("already started")]
    AlreadyStarted,

    /// Binary not found.
    #[error("binary not found")]
    BinaryNotFound,

    /// Cert store error.
    #[error("cert store error: {0}")]
    CertStore(#[from] proven_cert_store::Error),

    /// Failed to connect to nats server.
    #[error("failed to connect to nats server: {0}")]
    ClientFailedToConnect(#[from] async_nats::ConnectError),

    /// Governance error.
    #[error("governance error: {0}")]
    Governance(proven_governance::GovernanceErrorKind),

    /// IO error.
    #[error("io error ({0}): {1}")]
    Io(&'static str, #[source] io::Error),

    /// Error from the isolation crate.
    #[error("isolation error: {0}")]
    Isolation(#[from] proven_isolation::Error),

    /// Network error.
    #[error(transparent)]
    ProvenNetwork(#[from] proven_network::Error),

    /// Nats monitor error.
    #[error(transparent)]
    NatsMonitor(#[from] proven_nats_monitor::Error),

    /// Process exited with non-zero.
    #[error("nats server exited with non-zero status: {0}")]
    NonZeroExitCode(ExitStatus),

    /// Failed to parse nats server output.
    #[error("failed to parse nats server output")]
    OutputParse,

    /// Regex error
    #[error("regex error: {0}")]
    Regex(#[from] regex::Error),
}
