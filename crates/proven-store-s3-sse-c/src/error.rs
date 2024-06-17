use derive_more::From;

#[derive(Debug, From)]
pub enum Error {
    #[from]
    Io(std::io::Error),

    #[from]
    S3(aws_sdk_s3::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "{}", e),
            Error::S3(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}
