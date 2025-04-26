use axum::body::Body;
use axum::response::{IntoResponse as IntoAxumResponse, Response as AxumResponse};
use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};

/// A response from a SQL store.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Response {
    /// The body of the response.
    pub body: Bytes,

    /// The headers of the response.
    #[serde(with = "http_serde::header_map")]
    pub headers: HeaderMap,

    /// The status code of the response.
    #[serde(with = "http_serde::status_code")]
    pub status_code: StatusCode,
}

impl TryFrom<Bytes> for Response {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        ciborium::de::from_reader(bytes.as_ref())
    }
}

impl TryInto<Bytes> for Response {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&self, &mut bytes)?;
        Ok(Bytes::from(bytes))
    }
}

impl IntoAxumResponse for Response {
    fn into_response(self) -> AxumResponse {
        (self.status_code, self.headers, Body::from(self.body)).into_response()
    }
}
