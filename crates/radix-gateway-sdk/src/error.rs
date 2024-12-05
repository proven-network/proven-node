use thiserror::Error;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    LowLevel(#[from] crate::generated::Error<http::response::Response<httpclient::InMemoryBody>>),
}
