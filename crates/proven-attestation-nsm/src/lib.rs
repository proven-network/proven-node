mod error;
mod nsm;

pub use error::{Error, Result};

use std::sync::Arc;

use async_trait::async_trait;
use proven_attestation::{AttestationParams, Attestor};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct NsmAttestor {
    nsm: Arc<Mutex<nsm::Nsm>>,
}

impl NsmAttestor {
    pub fn new() -> Self {
        Self {
            nsm: Arc::new(Mutex::new(nsm::Nsm::new())),
        }
    }
}

#[async_trait]
impl Attestor for NsmAttestor {
    type AE = Error;

    async fn attest(&self, params: AttestationParams) -> Result<Vec<u8>> {
        self.nsm.lock().await.attestation(params)
    }

    async fn secure_random(&self) -> Result<Vec<u8>> {
        self.nsm.lock().await.get_random()
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
