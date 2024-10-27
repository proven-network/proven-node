use aws_nitro_enclaves_nsm_api::api::ErrorCode;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    UnexpectedResponse,
    RequestError(ErrorCode),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::UnexpectedResponse => write!(f, "unexpected response from nsm"),
            Error::RequestError(e) => write!(f, "nsm request error: {:?}", e),
        }
    }
}

impl std::error::Error for Error {}
