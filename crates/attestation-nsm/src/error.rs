use aws_nitro_enclaves_nsm_api::api::ErrorCode;
use proven_attestation::AttestorError;
use thiserror::Error;

/// The result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in this crate.
#[derive(Clone, Debug, Error)]
pub enum Error {
    /// Invalid argument to the NSM API.
    #[error("Invalid argument")]
    InvalidArgument,

    /// PCR index out of bounds.
    #[error("PCR index out of bounds")]
    InvalidIndex,

    /// Response does not match request.
    #[error("Response does not match request")]
    InvalidResponse,

    /// PCR is read-only.
    #[error("PCR is read-only")]
    ReadOnlyIndex,

    /// Missing capabilities.
    #[error("Missing capabilities")]
    InvalidOperation,

    /// Output buffer too small.
    #[error("Output buffer too small")]
    BufferTooSmall,

    /// Input too large.
    #[error("Input too large")]
    InputTooLarge,

    /// Internal NSM error.
    #[error("Internal NSM error")]
    InternalError,

    /// Unexpected response type from NSM.
    #[error("Unexpected response type from NSM")]
    UnexpectedResponse,
}

impl From<ErrorCode> for Error {
    fn from(code: ErrorCode) -> Self {
        match code {
            ErrorCode::Success => unreachable!("Success is not an error"),
            ErrorCode::InvalidArgument => Self::InvalidArgument,
            ErrorCode::InvalidIndex => Self::InvalidIndex,
            ErrorCode::InvalidResponse => Self::InvalidResponse,
            ErrorCode::ReadOnlyIndex => Self::ReadOnlyIndex,
            ErrorCode::InvalidOperation => Self::InvalidOperation,
            ErrorCode::BufferTooSmall => Self::BufferTooSmall,
            ErrorCode::InputTooLarge => Self::InputTooLarge,
            ErrorCode::InternalError => Self::InternalError,
        }
    }
}

impl AttestorError for Error {}
