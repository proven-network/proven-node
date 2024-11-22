use proven_store::StoreError;
use thiserror::Error;

#[derive(Clone, Debug, Error)]
#[error("Store error")]
pub struct Error;

impl StoreError for Error {}
