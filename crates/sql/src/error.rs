use std::sync::Arc;

use derive_more::From;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, From)]
pub enum Error {
    Libsql(Arc<libsql::Error>),

    #[from]
    Utf8(std::string::FromUtf8Error),
}

impl From<libsql::Error> for Error {
    fn from(error: libsql::Error) -> Self {
        Error::Libsql(Arc::new(error))
    }
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::Libsql(e) => write!(f, "Libsql error: {}", e),
            Error::Utf8(e) => write!(f, "Utf8 error: {}", e),
        }
    }
}

impl std::error::Error for Error {}
