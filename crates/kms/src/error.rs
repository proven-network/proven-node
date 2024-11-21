use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Kms(#[from] aws_sdk_kms::Error),

    #[error(transparent)]
    Nsm(#[from] proven_attestation_nsm::Error),

    #[error(transparent)]
    Rsa(#[from] rsa::errors::Error),

    #[error(transparent)]
    Spki(#[from] rsa::pkcs8::spki::Error),
}
