use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error<SE> {
    #[error("The server has already been started")]
    AlreadyStarted,

    #[error("Certificate store error: {0}")]
    CertStore(#[from] SE),
}
