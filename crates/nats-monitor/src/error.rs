use thiserror::Error;

/// The error type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// The error type for this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// An error occurred while making an HTTP request.
    #[error(transparent)]
    Http(#[from] httpclient::Error<http::response::Response<httpclient::InMemoryBody>>),

    /// An error occurred while parsing JSON.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
