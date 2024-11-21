use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Http(#[from] httpclient::Error<http::response::Response<httpclient::InMemoryBody>>),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
