//! Error types for the HTTP proxy engine.

use deno_error::{JsErrorClass, PropertyValue};
use std::borrow::Cow;
use thiserror::Error;

/// HTTP proxy engine errors.
#[derive(Error, Debug)]
pub enum Error {
    /// Client error
    #[error("Client error: {0}")]
    Client(String),

    /// Deserialization error
    #[error("Deserialization error: {0}")]
    Deserialization(String),

    /// Engine/Stream error
    #[error("Stream error: {0}")]
    Stream(String),

    /// Timeout error
    #[error("Timeout: {0}")]
    Timeout(String),

    /// Service error
    #[error("Service error: {0}")]
    Service(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(String),

    /// HTTP request error
    #[error("HTTP request error: {0}")]
    HttpRequest(String),

    /// Already started
    #[error("Already started")]
    AlreadyStarted,

    /// Channel closed
    #[error("Channel closed")]
    ChannelClosed,
}

impl JsErrorClass for Error {
    fn get_class(&self) -> Cow<'static, str> {
        match self {
            Self::Client(_) => Cow::Borrowed("HttpProxyClientError"),
            Self::Deserialization(_) => Cow::Borrowed("HttpProxyDeserializationError"),
            Self::Stream(_) => Cow::Borrowed("HttpProxyStreamError"),
            Self::Timeout(_) => Cow::Borrowed("HttpProxyTimeoutError"),
            Self::Service(_) => Cow::Borrowed("HttpProxyServiceError"),
            Self::Io(_) => Cow::Borrowed("HttpProxyIoError"),
            Self::HttpRequest(_) => Cow::Borrowed("HttpProxyHttpRequestError"),
            Self::AlreadyStarted => Cow::Borrowed("HttpProxyAlreadyStartedError"),
            Self::ChannelClosed => Cow::Borrowed("HttpProxyChannelClosedError"),
        }
    }

    fn get_message(&self) -> Cow<'static, str> {
        Cow::Owned(self.to_string())
    }

    fn get_additional_properties(
        &self,
    ) -> Box<dyn Iterator<Item = (Cow<'static, str>, PropertyValue)>> {
        Box::new(std::iter::empty())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl From<ciborium::ser::Error<std::io::Error>> for Error {
    fn from(err: ciborium::ser::Error<std::io::Error>) -> Self {
        Self::Deserialization(err.to_string())
    }
}

impl From<ciborium::de::Error<std::io::Error>> for Error {
    fn from(err: ciborium::de::Error<std::io::Error>) -> Self {
        Self::Deserialization(err.to_string())
    }
}

impl From<proven_engine::error::Error> for Error {
    fn from(err: proven_engine::error::Error) -> Self {
        Self::Stream(err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err.to_string())
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Self::HttpRequest(err.to_string())
    }
}
