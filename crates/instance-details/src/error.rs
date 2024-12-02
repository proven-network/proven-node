use thiserror::Error;

/// Result type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// Address parsing error.
    #[error(transparent)]
    AddrParse(#[from] std::net::AddrParseError),

    /// AWS SDK error.
    #[error(transparent)]
    EC2(#[from] aws_sdk_ec2::Error),

    /// Instance not found.
    #[error("instance not found")]
    InstanceNotFound,

    /// Missing instance details.
    #[error("missing instance details")]
    MissingDetails,
}
