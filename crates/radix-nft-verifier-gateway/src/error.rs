use proven_radix_nft_verifier::RadixNftVerifierError;
use thiserror::Error;

/// Error type for the NFT verifier.
#[derive(Debug, Error)]
pub enum Error {
    /// An error occurred while interacting with the Radix Gateway.
    #[error("Gateway error: {0}")]
    Gateway(proven_radix_gateway_sdk::Error<proven_radix_gateway_sdk::types::ErrorResponse>),
}

impl RadixNftVerifierError for Error {}
