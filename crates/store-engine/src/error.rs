//! Error types for store-engine

use std::any::Any;
use std::borrow::Cow;

use deno_error::{JsErrorClass, PropertyValue};
use proven_store::StoreError;
use thiserror::Error;

/// Errors that can occur in the engine store
#[derive(Clone, Debug, Error)]
pub enum Error {
    /// Engine client error
    #[error("Engine error: {0}")]
    Engine(String),

    /// Stream not found
    #[error("Stream not found: {0}")]
    StreamNotFound(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Deserialization error
    #[error("Deserialization error: {0}")]
    Deserialization(String),

    /// Key encoding error
    #[error("Key encoding error: {0}")]
    KeyEncoding(String),

    /// Operation timeout
    #[error("Operation timed out")]
    Timeout,

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

impl JsErrorClass for Error {
    fn get_class(&self) -> Cow<'static, str> {
        match self {
            Self::Engine(_) => Cow::Borrowed("EngineError"),
            Self::StreamNotFound(_) => Cow::Borrowed("NotFoundError"),
            Self::Serialization(_) | Self::Deserialization(_) | Self::KeyEncoding(_) => {
                Cow::Borrowed("TypeError")
            }
            Self::Timeout => Cow::Borrowed("TimeoutError"),
            Self::Internal(_) => Cow::Borrowed("InternalError"),
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

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl StoreError for Error {}
