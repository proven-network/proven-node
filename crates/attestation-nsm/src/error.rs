use aws_nitro_enclaves_nsm_api::api::ErrorCode;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid argument")]
    InvalidArgument,

    #[error("PCR index out of bounds")]
    InvalidIndex,

    #[error("Response does not match request")]
    InvalidResponse,

    #[error("PCR is read-only")]
    ReadOnlyIndex,

    #[error("Missing capabilities")]
    InvalidOperation,

    #[error("Output buffer too small")]
    BufferTooSmall,

    #[error("Input too large")]
    InputTooLarge,

    #[error("Internal NSM error")]
    InternalError,

    #[error("Unexpected response type from NSM")]
    UnexpectedResponse,
}

impl From<ErrorCode> for Error {
    fn from(code: ErrorCode) -> Self {
        match code {
            ErrorCode::Success => unreachable!("Success is not an error"),
            ErrorCode::InvalidArgument => Error::InvalidArgument,
            ErrorCode::InvalidIndex => Error::InvalidIndex,
            ErrorCode::InvalidResponse => Error::InvalidResponse,
            ErrorCode::ReadOnlyIndex => Error::ReadOnlyIndex,
            ErrorCode::InvalidOperation => Error::InvalidOperation,
            ErrorCode::BufferTooSmall => Error::BufferTooSmall,
            ErrorCode::InputTooLarge => Error::InputTooLarge,
            ErrorCode::InternalError => Error::InternalError,
        }
    }
}
