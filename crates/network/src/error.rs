//! Error types for the network implementation.

use thiserror::Error;

/// Result type for the network implementation.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for the network implementation.
#[derive(Debug, Error)]
pub enum Error {
    /// Attestation error.
    #[error("attestation error: {0}")]
    Attestation(String),

    /// Attestation verification failed.
    #[error("attestation verification failed: {0}")]
    AttestationVerificationFailed(String),

    /// Base64 decoding error.
    #[error("base64 decode error: {0}")]
    Base64Decode(String),

    /// Bad origin error.
    #[error("the origin is invalid")]
    BadOrigin,

    /// Error when generating the cluster endpoint.
    #[error("failed to generate cluster endpoint: {0}")]
    GenerateClusterEndpoint(&'static str),

    /// Hex decoding error.
    #[error("hex decode error: {0}")]
    HexDecode(String),

    /// Error from the governance implementation.
    #[error("governance error: {0}")]
    Governance(String),

    /// Error when a node is not found in the topology.
    #[error("node not found: {0}")]
    NodeNotFound(String),

    /// Error when encoding the nonce.
    #[error("nonce encoding error")]
    NonceEncoding,

    /// Error related to private key operations.
    #[error("private key error: {0}")]
    PrivateKey(String),

    /// Error from reqwest.
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    /// Request failed with non-200 status code.
    #[error("request failed with status code: {0}")]
    RequestFailed(u16),

    /// Invalid header value error.
    #[error("invalid header value: {0}")]
    InvalidHeader(&'static str),

    /// Invalid public key format error.
    #[error("invalid public key format: {0}")]
    InvalidPublicKeyFormat(String),

    /// Invalid signature format error.
    #[error("invalid signature format: {0}")]
    InvalidSignatureFormat(String),

    /// Missing header error.
    #[error("missing required header: {0}")]
    MissingHeader(&'static str),

    /// Signature verification failed.
    #[error("signature verification failed: {0}")]
    SignatureVerificationFailed(String),

    /// URL parsing error.
    #[error(transparent)]
    Url(#[from] url::ParseError),

    /// UTF-8 decoding error.
    #[error(transparent)]
    Utf8Decode(#[from] std::string::FromUtf8Error),
}
