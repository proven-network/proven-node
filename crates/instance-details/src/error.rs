use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    AddrParse(#[from] std::net::AddrParseError),

    #[error("{0}")]
    EC2(#[from] aws_sdk_ec2::Error),

    #[error("instance not found")]
    InstanceNotFound,

    #[error("missing instance details")]
    MissingDetails,
}
