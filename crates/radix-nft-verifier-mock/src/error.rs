use deno_error::JsError;
use proven_radix_nft_verifier::RadixNftVerifierError;
use thiserror::Error;

/// Error type for the mock NFT verifier.
#[derive(Debug, Error, JsError)]
#[class(generic)]
#[error("mock verifier error")]
pub struct Error;

impl RadixNftVerifierError for Error {}
