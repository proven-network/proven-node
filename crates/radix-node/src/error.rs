pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    AlreadyStarted,
    CaCertsBadPath,
    CaCertsWrite(std::io::Error),
    ConfigWrite(std::io::Error),
    Crashed,
    OutputParse,
    NonZeroExitCode(std::process::ExitStatus),
    Spawn(std::io::Error),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::AlreadyStarted => write!(f, "already started"),
            Error::CaCertsBadPath => write!(f, "bad path"),
            Error::CaCertsWrite(e) => write!(f, "failed to write cacerts: {}", e),
            Error::ConfigWrite(e) => write!(f, "failed to write config: {}", e),
            Error::Crashed => write!(f, "crashed"),
            Error::NonZeroExitCode(status) => {
                write!(f, "exited with non-zero: {}", status)
            }
            Error::OutputParse => write!(f, "failed to parse output"),
            Error::Spawn(e) => write!(f, "failed to spawn: {}", e),
        }
    }
}

impl std::error::Error for Error {}