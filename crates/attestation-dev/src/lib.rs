//! Noop implementation of attestation for local development.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;

use bytes::Bytes;
use coset::CborSerializable;
pub use error::{Error, Result};

use std::collections::BTreeMap;

use async_trait::async_trait;
use proven_attestation::{AttestationParams, Attestor};
use rand::RngCore;
use serde::Serialize;

/// Noop attestation provider for local development.
#[derive(Clone, Debug, Default)]
pub struct DevAttestor;

#[derive(Clone, Debug, Default, Serialize)]
struct Attestation {
    pcrs: BTreeMap<u8, Bytes>,
    nonce: Bytes,
    user_data: Bytes,
    public_key: Bytes,
}

#[async_trait]
impl Attestor for DevAttestor {
    type Error = Error;

    async fn attest(&self, params: AttestationParams) -> Result<Bytes> {
        // use zerod pcrs in dev mode
        let mut pcrs: BTreeMap<u8, Bytes> = BTreeMap::new();
        for i in 0..4 {
            pcrs.insert(i, Bytes::from(vec![0; 32]));
        }
        pcrs.insert(8, Bytes::from(vec![0; 32]));

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

        let attestation_document = Bytes::from(sign1.to_vec()?);

        Ok(attestation_document)
    }

    async fn secure_random(&self) -> Result<Bytes> {
        let mut rng = rand::rngs::OsRng;
        let mut random = vec![0; 32];
        rng.fill_bytes(&mut random);
        Ok(Bytes::from(random))
    }
}

impl TryInto<Bytes> for Attestation {
    type Error = Error;

    fn try_into(self) -> Result<Bytes> {
        let mut payload = Vec::new();
        ciborium::ser::into_writer(&self, &mut payload).map_err(|_| Error::Cbor)?;
        Ok(Bytes::from(payload))
    }
}
