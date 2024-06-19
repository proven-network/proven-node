pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    AlreadyStarted,
    ConfigWrite(std::io::Error),
    OutputParse,
    NonZeroExitCode(std::process::ExitStatus),
    Spawn(std::io::Error),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::AlreadyStarted => write!(f, "dnscrypt-proxy already started"),
            Error::ConfigWrite(e) => write!(f, "failed to write nats config: {}", e),
            Error::NonZeroExitCode(status) => {
                write!(f, "dnscrypt-proxy exited with non-zero: {}", status)
            }
            Error::OutputParse => write!(f, "failed to parse dnscrypt-proxy output"),
            Error::Spawn(e) => write!(f, "failed to spawn dnscrypt-proxy: {}", e),
        }
    }
}

impl std::error::Error for Error {}
