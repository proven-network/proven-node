use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("already started")]
    AlreadyStarted,

    #[error("failed to write config: {0}")]
    ConfigWrite(std::io::Error),

    #[error("node exited before ready")]
    ExitBeforeReady,

    #[error("exited with non-zero: {0}")]
    NonZeroExitCode(std::process::ExitStatus),

    #[error("failed to parse output")]
    OutputParse,

    #[error("failed to spawn: {0}")]
    Spawn(std::io::Error),
}
