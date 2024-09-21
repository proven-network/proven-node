use derive_more::From;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    NetworkInvalid,

    #[from]
    LowLevel(crate::generated::Error<http::response::Response<httpclient::InMemoryBody>>),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::NetworkInvalid => write!(f, "Network invalid"),
            Error::LowLevel(e) => write!(f, "Request error: {}", e),
        }
    }
}

impl std::error::Error for Error {}
