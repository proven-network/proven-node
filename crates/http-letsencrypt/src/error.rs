use proven_http::HttpServerError;
use proven_store::StoreError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error<SE>
where
    SE: StoreError,
{
    #[error("The server has already been started")]
    AlreadyStarted,

    #[error("Certificate store error: {0}")]
    CertStore(#[from] SE),
}

impl<SE> HttpServerError for Error<SE> where SE: StoreError {}
