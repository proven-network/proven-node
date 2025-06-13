use thiserror::Error;

/// Errors that can occur during RPC operations
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid payload
    #[error("cannot deserialize payload")]
    Deserialize,

    #[error("cannot serialize payload")]
    Serialize,

    /// Invalid COSE Sign1
    #[error("Invalid COSE_Sign1")]
    Sign1,

    /// Invalid signature
    #[error("Invalid signature")]
    Signature,
}
