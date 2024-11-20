mod error;

use coset::CborSerializable;
use error::{Error, Result};

use std::collections::BTreeMap;

use async_trait::async_trait;
use proven_attestation::{AttestationParams, Attestor};
use rand::RngCore;
use serde::Serialize;

#[derive(Clone, Debug, Default)]
pub struct DevAttestor {}

impl DevAttestor {
    pub fn new() -> Self {
        Self {}
    }
}

#[derive(Clone, Debug, Default, Serialize)]
struct Attestation {
    pcrs: BTreeMap<u8, Vec<u8>>,
    nonce: Vec<u8>,
    user_data: Vec<u8>,
    public_key: Vec<u8>,
}

#[async_trait]
impl Attestor for DevAttestor {
    type AE = Error;

    async fn attest(&self, params: AttestationParams) -> Result<Vec<u8>> {
        // use zerod pcrs in dev mode
        let mut pcrs = BTreeMap::new();
        for i in 0..32 {
            pcrs.insert(i, vec![0; 32]);
        }

        let attestation = Attestation {
            pcrs,
            nonce: params.nonce.unwrap_or_default(),
            user_data: params.user_data.unwrap_or_default(),
            public_key: params.public_key.unwrap_or_default(),
        };

        let mut payload = Vec::new();
        ciborium::ser::into_writer(&attestation, &mut payload).map_err(|_| Error::Cbor)?;

        let sign1 = coset::CoseSign1Builder::new()
            .payload(payload)
            .create_signature(b"", |_| vec![0; 64])
            .build();

        let attestation_document = sign1.to_vec()?;

        Ok(attestation_document)
    }

    async fn secure_random(&self) -> Result<Vec<u8>> {
        let mut rng = rand::rngs::OsRng;
        let mut random = vec![0; 32];
        rng.fill_bytes(&mut random);
        Ok(random)
    }
}
