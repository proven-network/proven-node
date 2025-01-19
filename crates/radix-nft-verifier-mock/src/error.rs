use std::fmt;

use proven_radix_nft_verifier::RadixNftVerifierError;

/// Error type for the mock NFT verifier.
#[derive(Debug)]
pub struct Error;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "mock verifier error")
    }
}

impl std::error::Error for Error {}
impl RadixNftVerifierError for Error {}
