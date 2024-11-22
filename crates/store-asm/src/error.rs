use proven_store::StoreError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    SecretsManager(#[from] aws_sdk_secretsmanager::Error),
}

impl StoreError for Error {}
