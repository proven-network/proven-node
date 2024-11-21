use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("hash not known to pool")]
    HashUnknown,

    #[error(transparent)]
    RustyScript(#[from] rustyscript::Error),
}
