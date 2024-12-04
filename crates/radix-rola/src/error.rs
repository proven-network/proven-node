use radix_common::crypto::{ParseEd25519SignatureError, ParseSecp256k1SignatureError};
use thiserror::Error;

/// The result type for this crate.
pub type Result<T> = core::result::Result<T, Error>;

/// An error that can occur in this crate.
#[derive(Debug, Error)]
pub enum Error {
    /// The address has no owner keys.
    #[error("Address has no owner keys")]
    AddressHasNoOwnerKeys,

    /// The address was not found.
    #[error("Address not found")]
    AddressNotFound,

    /// Bad Ed25519 signature.
    #[error(transparent)]
    BadEd25519Signature(#[from] ParseEd25519SignatureError),

    /// Bad gateway request.
    #[error(transparent)]
    BadGatewayRequest(
        #[from] proven_radix_gateway_sdk::Error<proven_radix_gateway_sdk::types::ErrorResponse>,
    ),

    /// Bad Secp256k1 signature.
    #[error(transparent)]
    BadSecp256k1Signature(#[from] ParseSecp256k1SignatureError),

    /// Bech32 error.
    #[error(transparent)]
    CouldNotEncodeBech32(#[from] bech32::EncodeError),

    /// Bech32 error.
    #[error(transparent)]
    CouldNotParseHrp(#[from] bech32::primitives::hrp::Error),

    /// Curve does not match key.
    #[error("Curve does not match key")]
    CurveDoesNotMatchKey,

    /// Failed verification.
    #[error("Failed verification")]
    FailedVerification,

    /// Invalid public key.
    #[error("Invalid public key")]
    InvalidPublicKey,

    /// Invalid proof.
    #[error("Invalid proof")]
    InvalidProof,
}
