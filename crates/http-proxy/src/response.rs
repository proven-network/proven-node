//! HTTP proxy response types.

use axum::body::Body;
use axum::response::{IntoResponse as IntoAxumResponse, Response as AxumResponse};
use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A response from the HTTP proxy.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Response {
    /// The request ID this response is for.
    pub request_id: Uuid,

    /// The body of the response.
    pub body: Bytes,

    /// The headers of the response.
    #[serde(with = "http_serde::header_map")]
    pub headers: HeaderMap,

    /// The status code of the response.
    #[serde(with = "http_serde::status_code")]
    pub status_code: StatusCode,
}

impl IntoAxumResponse for Response {
    fn into_response(self) -> AxumResponse {
        (self.status_code, self.headers, Body::from(self.body)).into_response()
    }
}
