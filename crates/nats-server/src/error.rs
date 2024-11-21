use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("nats server already started")]
    AlreadyStarted,

    #[error("failed to connect to nats server: {0}")]
    ClientFailedToConnect(#[from] async_nats::ConnectError),

    #[error("failed to write nats config: {0}")]
    ConfigWrite(#[from] std::io::Error),

    #[error("nats server exited with non-zero status: {0}")]
    NonZeroExitCode(std::process::ExitStatus),

    #[error("failed to parse nats server output")]
    OutputParse,

    #[error("failed to spawn nats server: {0}")]
    Spawn(std::io::Error),
}
