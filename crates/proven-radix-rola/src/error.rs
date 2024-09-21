use derive_more::From;

pub type Result<T> = core::result::Result<T, Error>;

use radix_common::crypto::{ParseEd25519SignatureError, ParseSecp256k1SignatureError};

#[derive(Debug, From)]
pub enum Error {
    InvalidProof,
    InvalidPublicKey,

    // Gateway/Data
    AddressNotFound,
    AddressHasNoOwnerKeys,

    #[from]
    BadGatewayRequest(proven_radix_gateway_sdk::Error),

    // Signature Verification
    CurveDoesNotMatchKey,
    FailedVerification,

    #[from]
    BadEd25519Signature(ParseEd25519SignatureError),
    #[from]
    BadSecp256k1Signature(ParseSecp256k1SignatureError),

    // Virtual address derivation
    #[from]
    CouldNotEncodeBech32(bech32::EncodeError),

    #[from]
    CouldNotParseHrp(bech32::primitives::hrp::Error),
}

impl core::fmt::Display for Error {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::InvalidProof => write!(f, "Invalid proof"),
            Error::InvalidPublicKey => write!(f, "Invalid public key"),
            Error::AddressNotFound => write!(f, "Address not found"),
            Error::AddressHasNoOwnerKeys => write!(f, "Owner key missing"),
            Error::BadGatewayRequest(e) => write!(f, "Bad Gateway reqest: {}", e),
            Error::CurveDoesNotMatchKey => write!(f, "Curve does not match key"),
            Error::FailedVerification => write!(f, "Failed verification"),
            Error::BadEd25519Signature(e) => write!(f, "Bad Ed25519 signature: {}", e),
            Error::BadSecp256k1Signature(e) => write!(f, "Bad Secp256k1 signature: {}", e),
            Error::CouldNotEncodeBech32(e) => write!(f, "Could not encode bech32: {}", e),
            Error::CouldNotParseHrp(e) => write!(f, "Could not parse HRP: {}", e),
        }
    }
}

impl std::error::Error for Error {}
