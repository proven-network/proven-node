use derive_more::From;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    #[from]
    Io(std::io::Error),

    #[from]
    Kms(aws_sdk_kms::Error),

    #[from]
    Nsm(proven_attestation_nsm::Error),

    #[from]
    Rsa(rsa::errors::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "{}", e),
            Error::Kms(e) => write!(f, "{}", e),
            Error::Nsm(e) => write!(f, "{}", e),
            Error::Rsa(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}