use async_nats::ConnectError as ClientConnectError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    AlreadyStarted,
    ClientFailedToConnect(ClientConnectError),
    ConfigWrite(std::io::Error),
    OutputParse,
    NonZeroExitCode(std::process::ExitStatus),
    Spawn(std::io::Error),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::AlreadyStarted => write!(f, "nats server already started"),
            Error::ConfigWrite(e) => write!(f, "failed to write nats config: {}", e),
            Error::ClientFailedToConnect(e) => {
                write!(f, "failed to connect to nats server: {}", e)
            }
            Error::NonZeroExitCode(status) => {
                write!(f, "nats server exited with non-zero status: {}", status)
            }
            Error::OutputParse => write!(f, "failed to parse nats server output"),
            Error::Spawn(e) => write!(f, "failed to spawn nats server: {}", e),
        }
    }
}

impl std::error::Error for Error {}
