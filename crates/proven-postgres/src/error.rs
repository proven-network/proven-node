pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    AlreadyStarted,
    ConfigWrite(std::io::Error),
    OutputParse,
    NonZeroExitCode(std::process::ExitStatus),
    Spawn(std::io::Error),
    VacuumFailed,
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::AlreadyStarted => write!(f, "already started"),
            Error::ConfigWrite(e) => write!(f, "failed to write config: {}", e),
            Error::NonZeroExitCode(status) => {
                write!(f, "exited with non-zero: {}", status)
            }
            Error::OutputParse => write!(f, "failed to parse output"),
            Error::Spawn(e) => write!(f, "failed to spawn: {}", e),
            Error::VacuumFailed => write!(f, "vacuum failed"),
        }
    }
}

impl std::error::Error for Error {}
