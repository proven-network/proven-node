use derive_more::From;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    Custom(String),

    #[from]
    Base64(base64::DecodeError),

    #[from]
    Http(httpclient::Error<http::response::Response<httpclient::InMemoryBody>>),

    #[from]
    Json(serde_json::Error),

    #[from]
    Pem(::pem::PemError),

    #[from]
    X509(x509_parser::nom::Err<x509_parser::error::X509Error>),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::Custom(e) => write!(f, "{}", e),
            Error::Base64(e) => write!(f, "{}", e),
            Error::Http(e) => write!(f, "{}", e),
            Error::Json(e) => write!(f, "{}", e),
            Error::Pem(e) => write!(f, "{}", e),
            Error::X509(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}
