use derive_more::From;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    #[from]
    AddrParse(std::net::AddrParseError),

    #[from]
    EC2(aws_sdk_ec2::Error),

    InstanceNotFound,
    MissingDetails,
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::AddrParse(e) => write!(f, "{}", e),
            Error::EC2(e) => write!(f, "{}", e),
            Error::InstanceNotFound => write!(f, "instance not found"),
            Error::MissingDetails => write!(f, "missing instance details"),
        }
    }
}

impl std::error::Error for Error {}
