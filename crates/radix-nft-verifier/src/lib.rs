//! Abstract interface for verifying Radix NFT ownership.
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use deno_error::JsErrorClass;

/// Marker trait for NFT verifier errors.
pub trait RadixNftVerifierError: Error + JsErrorClass + Send + Sync + 'static {}

/// Verification result.
pub enum RadixNftVerificationResult {
    /// The NFT is owned by one of the provided accounts.
    Owned(String), // The account that owns the NFT.

    /// The NFT does not exist.
    NftDoesNotExist,

    /// The NFT is not owned by any of the provided accounts.
    NotOwned(String), // The account that owns the NFT.
}

/// A trait representing a Radix NFT verifier with asynchronous operations.
#[async_trait]
pub trait RadixNftVerifier
where
    Self: Clone + Debug + Send + Sync + 'static,
{
    /// The error type for the NFT verifier.
    type Error: RadixNftVerifierError;

    /// Checks whether one of the given accounts owns the NFT currently.
    async fn verify_ownership<A, R, N>(
        &self,
        accounts: &[A],
        resource_address: R,
        nft_id: N,
    ) -> Result<RadixNftVerificationResult, Self::Error>
    where
        A: Clone + Into<String> + Send + Sync,
        R: Clone + Into<String> + Send + Sync,
        N: Clone + Into<String> + Send + Sync;
}
