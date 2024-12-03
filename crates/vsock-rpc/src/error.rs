use thiserror::Error;

/// Result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// The response type was not as expected.
    #[error("bad response type")]
    BadResponseType,

    /// CBOR deserialization error.
    #[error(transparent)]
    CborDeserialize(#[from] ciborium::de::Error<std::io::Error>),

    /// CBOR serialization error.
    #[error(transparent)]
    CborSerialize(#[from] ciborium::ser::Error<std::io::Error>),

    /// IO operation failed.
    #[error("{0}: {1}")]
    Io(&'static str, #[source] std::io::Error),

    /// Response too large for wire format.
    #[error("response too large: was {0} expected less than or equal to {1}")]
    ResponseTooLarge(usize, u32),
}
