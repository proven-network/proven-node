//! Abstract interface for managing remote attestation and interacting with hardware-based security modules.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;
/// Parameters for attestation.
pub struct AttestationParams {
    /// Optional challenge to include in the attestation.
    pub nonce: Option<Bytes>,

    /// Optional user data to include in the attestation.
    pub user_data: Option<Bytes>,

    /// Optional public key to include in the attestation.
    pub public_key: Option<Bytes>,
}

/// PCR values.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Pcrs {
    /// PCR 0
    pub pcr0: Bytes,

    /// PCR 1
    pub pcr1: Bytes,

    /// PCR 2
    pub pcr2: Bytes,

    /// PCR 3
    pub pcr3: Bytes,

    /// PCR 4
    pub pcr4: Bytes,

    /// PCR 8
    pub pcr8: Bytes,
}

/// Verified attestation.
pub struct VerifiedAttestation {
    /// An optional cryptographic nonce provided by the attestation consumer as a proof of
    /// authenticity.
    pub nonce: Option<Bytes>,

    /// The PCRs.
    pub pcrs: Pcrs,

    /// An optional DER-encoded key the attestation consumer can use to encrypt data with
    pub public_key: Option<Bytes>,

    /// Additional signed user data, as defined by protocol.
    pub user_data: Option<Bytes>,
}

/// Marker trait for `Attestor` errors.
pub trait AttestorError: Clone + Debug + Error + Send + Sync {}

/// Trait for remote attestation provider.
#[async_trait]
pub trait Attestor
where
    Self: Clone + Send + Sync + 'static,
{
    /// The error type for the attestation provider.
    type Error: AttestorError;

    /// Attest to the authenticity of the device.
    async fn attest(&self, params: AttestationParams) -> Result<Bytes, Self::Error>;

    /// Get the PCRs for the device.
    async fn pcrs(&self) -> Result<Pcrs, Self::Error>;

    /// Generate secure random bytes.
    async fn secure_random(&self) -> Result<Bytes, Self::Error>;

    /// Verify an attestation document.
    ///
    /// # Errors
    ///
    /// Returns an error if the attestation document is invalid or verification fails.
    fn verify(&self, attestation: Bytes) -> Result<VerifiedAttestation, Self::Error>;
}
