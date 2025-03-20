use thiserror::Error;

/// The result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Already started.
    #[error("nats server already started")]
    AlreadyStarted,

    /// Bad PID.
    #[error("bad PID")]
    BadPid,

    /// Failed to connect to nats server.
    #[error("failed to connect to nats server: {0}")]
    ClientFailedToConnect(#[from] async_nats::ConnectError),

    /// Governance error.
    #[error("governance error: {0}")]
    Governance(proven_governance::GovernanceErrorKind),

    /// IO operation failed.
    #[error("{0}: {1}")]
    Io(&'static str, #[source] std::io::Error),

    /// Network error.
    #[error(transparent)]
    ProvenNetwork(#[from] proven_network::Error),

    /// Process exited with non-zero.
    #[error("nats server exited with non-zero status: {0}")]
    NonZeroExitCode(std::process::ExitStatus),

    /// Failed to parse nats server output.
    #[error("failed to parse nats server output")]
    OutputParse,

    /// Failed to parse regex pattern.
    #[error(transparent)]
    RegexParse(#[from] regex::Error),
}
