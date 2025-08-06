//! Error types for store-engine

use std::any::Any;
use std::borrow::Cow;

use deno_error::{JsErrorClass, PropertyValue};
use proven_store::StoreError;
use thiserror::Error;

/// Store engine error type
#[derive(Error, Debug)]
pub enum Error {
    /// Engine error
    #[error("Engine error: {0}")]
    Engine(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Deserialization error  
    #[error("Deserialization error: {0}")]
    Deserialization(String),

    /// Stream error
    #[error("Stream error: {0}")]
    Stream(String),

    /// Other error
    #[error("Other error: {0}")]
    Other(String),
}

impl From<proven_engine::error::Error> for Error {
    fn from(err: proven_engine::error::Error) -> Self {
        Self::Engine(err.to_string())
    }
}

impl JsErrorClass for Error {
    fn get_class(&self) -> Cow<'static, str> {
        match self {
            Self::Engine(_) => Cow::Borrowed("EngineError"),
            Self::Serialization(_) => Cow::Borrowed("SerializationError"),
            Self::Deserialization(_) => Cow::Borrowed("DeserializationError"),
            Self::Stream(_) => Cow::Borrowed("StreamError"),
            Self::Other(_) => Cow::Borrowed("Error"),
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
