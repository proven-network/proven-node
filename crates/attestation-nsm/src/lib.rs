//! Implementation of attestation using the Nitro Security Module.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod nsm;

pub use error::{Error, Result};

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_attestation::{AttestationParams, Attestor};
use tokio::sync::Mutex;

/// Attestor implementation using the Nitro Security Module.
#[derive(Debug)]
pub struct NsmAttestor {
    nsm: Arc<Mutex<nsm::Nsm>>,
}

impl NsmAttestor {
    /// Create a new `NsmAttestor`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            nsm: Arc::new(Mutex::new(nsm::Nsm::new())),
        }
    }
}

#[async_trait]
impl Attestor for NsmAttestor {
    type Error = Error;

    async fn attest(&self, params: AttestationParams) -> Result<Bytes> {
        Ok(Bytes::from(self.nsm.lock().await.attestation(params)?))
    }

    async fn secure_random(&self) -> Result<Bytes> {
        Ok(Bytes::from(self.nsm.lock().await.get_random()?))
    }
}

impl Clone for NsmAttestor {
    fn clone(&self) -> Self {
        Self {
            nsm: Arc::clone(&self.nsm),
        }
    }
}

impl Default for NsmAttestor {
    fn default() -> Self {
        Self::new()
    }
}
