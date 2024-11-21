use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("external fs already started")]
    AlreadyStarted,

    #[error("external fs exited with non-zero: {0}")]
    NonZeroExitCode(std::process::ExitStatus),

    #[error("failed to parse external fs output")]
    OutputParse,

    #[error("failed to spawn: {0}")]
    Spawn(#[from] std::io::Error),
}
