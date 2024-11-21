use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Base64(#[from] base64::DecodeError),

    #[error(transparent)]
    Http(#[from] httpclient::Error<http::response::Response<httpclient::InMemoryBody>>),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Pem(#[from] ::pem::PemError),

    #[error("Signature verification failed: {0}")]
    SignatureVerification(String),

    #[error(transparent)]
    X509(#[from] x509_parser::nom::Err<x509_parser::error::X509Error>),
}
