//! Error types for the identity-engine crate.

use std::fmt;

/// Error type for identity operations.
#[derive(Debug)]
pub enum Error {
    /// Client error.
    Client(String),

    /// Command processing error.
    Command(String),

    /// Deserialization error.
    Deserialization(String),

    /// Serialization error.
    Serialization(String),

    /// Service error.
    Service(String),

    /// Stream error.
    Stream(String),

    /// Timeout error.
    Timeout,

    /// Unexpected response type.
    UnexpectedResponse,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(msg) => write!(f, "Client error: {msg}"),
            Self::Command(msg) => write!(f, "Command error: {msg}"),
            Self::Deserialization(msg) => write!(f, "Deserialization error: {msg}"),
            Self::Serialization(msg) => write!(f, "Serialization error: {msg}"),
            Self::Service(msg) => write!(f, "Service error: {msg}"),
            Self::Stream(msg) => write!(f, "Stream error: {msg}"),
            Self::Timeout => write!(f, "Operation timed out"),
            Self::UnexpectedResponse => write!(f, "Unexpected response type"),
        }
    }
}

impl std::error::Error for Error {}

impl From<ciborium::ser::Error<std::io::Error>> for Error {
    fn from(err: ciborium::ser::Error<std::io::Error>) -> Self {
        Self::Serialization(err.to_string())
    }
}

impl From<ciborium::de::Error<std::io::Error>> for Error {
    fn from(err: ciborium::de::Error<std::io::Error>) -> Self {
        Self::Deserialization(err.to_string())
    }
}
