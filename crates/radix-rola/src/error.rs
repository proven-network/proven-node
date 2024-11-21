use radix_common::crypto::{ParseEd25519SignatureError, ParseSecp256k1SignatureError};
use thiserror::Error;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Address has no owner keys")]
    AddressHasNoOwnerKeys,

    #[error("Address not found")]
    AddressNotFound,

    #[error(transparent)]
    BadEd25519Signature(#[from] ParseEd25519SignatureError),

    #[error(transparent)]
    BadGatewayRequest(#[from] proven_radix_gateway_sdk::Error),

    #[error(transparent)]
    BadSecp256k1Signature(#[from] ParseSecp256k1SignatureError),

    #[error(transparent)]
    CouldNotEncodeBech32(#[from] bech32::EncodeError),

    #[error(transparent)]
    CouldNotParseHrp(#[from] bech32::primitives::hrp::Error),

    #[error("Curve does not match key")]
    CurveDoesNotMatchKey,

    #[error("Failed verification")]
    FailedVerification,

    #[error("Invalid proof")]
    InvalidProof,

    #[error("Invalid public key")]
    InvalidPublicKey,
}
