use proven_store::StoreError;
use thiserror::Error;

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Error)]
#[error("Store error")]
pub struct Error;

impl StoreError for Error {}
