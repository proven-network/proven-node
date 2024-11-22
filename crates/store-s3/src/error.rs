use proven_store::StoreError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    S3(#[from] aws_sdk_s3::Error),
}

impl StoreError for Error {}
