use derive_more::From;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    HashUnknown,

    #[from]
    RustyScript(rustyscript::Error),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::HashUnknown => write!(f, "Hash not known"),
            Error::RustyScript(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}
