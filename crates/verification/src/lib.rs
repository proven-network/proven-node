//! Verification module for secure peer authentication and message signing
//!
//! This crate provides:
//! - Connection verification with mutual authentication handshake
//! - COSE (CBOR Object Signing and Encryption) message signing
//! - Attestation-based peer verification
//!
//! The verification system ensures that all peers in the network are properly
//! authenticated and running authorized software versions.

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod attestation;
pub mod connection;
pub mod cose;
pub mod error;

// Re-export main types
pub use attestation::AttestationVerifier;
pub use connection::{ConnectionState, ConnectionVerifier, VerificationMessage};
pub use cose::{CoseHandler, CoseMetadata};
pub use error::{VerificationError, VerificationResult};

// Re-export from dependencies for convenience
pub use bytes::Bytes;
pub use cose::CoseSign1;
