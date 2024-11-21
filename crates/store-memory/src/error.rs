use thiserror::Error;

#[derive(Clone, Debug, Error)]
#[error("Store error")]
pub struct Error;
