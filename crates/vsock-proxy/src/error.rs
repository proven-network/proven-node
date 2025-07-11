use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("proxy already started")]
    AlreadyStarted,

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("non-zero exit code: {0}")]
    NonZeroExitCode(std::process::ExitStatus),

    #[error("failed to parse output")]
    OutputParse,

    #[cfg(target_os = "linux")]
    #[error("failed to send signal")]
    Signal(#[source] nix::errno::Errno),

    #[error("failed to spawn process")]
    Spawn(#[source] std::io::Error),
}
