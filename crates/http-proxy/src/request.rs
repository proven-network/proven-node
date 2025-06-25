use bytes::Bytes;
use http::{HeaderMap, Method};
use serde::{Deserialize, Serialize};

/// A request to a SQL store.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Request {
    /// The body of the request.
    pub body: Option<Bytes>,

    /// The headers of the request.
    #[serde(with = "http_serde::header_map")]
    pub headers: HeaderMap,

    /// The method of the request.
    #[serde(with = "http_serde::method")]
    pub method: Method,

    /// The path of the request.
    pub path: String,
}

impl TryFrom<Bytes> for Request {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        ciborium::de::from_reader(bytes.as_ref())
    }
}

impl TryInto<Bytes> for Request {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&self, &mut bytes)?;
        Ok(Bytes::from(bytes))
    }
}
