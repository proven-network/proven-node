//! Error types for the applications-engine crate.

use std::fmt;

/// Errors that can occur in the applications manager.
#[derive(Debug, Clone)]
pub enum Error {
    /// Client error when sending commands
    Client(String),
    /// Command processing error
    Command(String),
    /// Deserialization error
    Deserialization(String),
    /// Serialization error
    Serialization(String),
    /// Service error
    Service(String),
    /// Stream operation error
    Stream(String),
    /// Unexpected response type
    UnexpectedResponse,
    /// Timeout waiting for response
    Timeout,
    /// Consumer error
    Consumer(String),
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
            Self::UnexpectedResponse => write!(f, "Unexpected response type"),
            Self::Timeout => write!(f, "Timeout waiting for response"),
            Self::Consumer(msg) => write!(f, "Consumer error: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<ciborium::de::Error<std::io::Error>> for Error {
    fn from(err: ciborium::de::Error<std::io::Error>) -> Self {
        Self::Deserialization(err.to_string())
    }
}

impl From<ciborium::ser::Error<std::io::Error>> for Error {
    fn from(err: ciborium::ser::Error<std::io::Error>) -> Self {
        Self::Serialization(err.to_string())
    }
}
