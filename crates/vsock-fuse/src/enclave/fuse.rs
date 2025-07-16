//! FUSE filesystem implementation for the enclave
//!
//! This is a re-export and thin wrapper around the core FUSE implementation
//! to provide enclave-specific functionality.

pub use crate::fuse::VsockFuseFs;

// Additional enclave-specific FUSE extensions can be added here
