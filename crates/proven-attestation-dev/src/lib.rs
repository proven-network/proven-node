mod error;

use error::{Error, Result};

use async_trait::async_trait;
use proven_attestation::{AttestationParams, Attestor};
use rand::RngCore;

#[derive(Clone, Debug, Default)]
pub struct DevAttestor {}

impl DevAttestor {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Attestor for DevAttestor {
    type AE = Error;

    async fn attest(&self, _params: AttestationParams) -> Result<Vec<u8>> {
        let attestation = vec![0; 0];
        Ok(attestation)
    }

    async fn secure_random(&self) -> Result<Vec<u8>> {
        let mut rng = rand::rngs::OsRng;
        let mut random = vec![0; 32];
        rng.fill_bytes(&mut random);
        Ok(random)
    }
}
