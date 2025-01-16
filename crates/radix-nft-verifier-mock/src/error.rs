use std::fmt;

use proven_radix_nft_verifier::RadixNftVerifierError;

/// Error type for the mock NFT verifier.
#[derive(Debug)]
pub struct RadixNftVerifierErrorMock;

impl fmt::Display for RadixNftVerifierErrorMock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "mock verifier error")
    }
}

impl std::error::Error for RadixNftVerifierErrorMock {}
impl RadixNftVerifierError for RadixNftVerifierErrorMock {}
