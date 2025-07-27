//! HTTP proxy request types.

use bytes::Bytes;
use http::{HeaderMap, Method};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A request to the HTTP proxy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Request {
    /// The request ID for correlation.
    pub id: Uuid,

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
