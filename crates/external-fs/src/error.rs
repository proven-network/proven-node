use derive_more::From;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    AlreadyStarted,
    OutputParse,
    NonZeroExitCode(std::process::ExitStatus),
    Spawn(std::io::Error),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::AlreadyStarted => write!(f, "external fs already started"),
            Error::NonZeroExitCode(status) => {
                write!(f, "external fs exited with non-zero: {}", status)
            }
            Error::OutputParse => write!(f, "failed to parse external fs output"),
            Error::Spawn(e) => write!(f, "failed to spawn: {}", e),
        }
    }
}

impl std::error::Error for Error {}
