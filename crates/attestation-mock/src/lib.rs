//! Noop implementation of attestation for local development.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;

pub use error::{Error, Result};

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use nsm_nitro_enclave_utils::api::nsm::{AttestationDoc, Request, Response};
use nsm_nitro_enclave_utils::api::{DecodePrivateKey, SecretKey};
use nsm_nitro_enclave_utils::driver::Driver;
use nsm_nitro_enclave_utils::driver::dev::DevNitro;
use nsm_nitro_enclave_utils::pcr::{PcrIndex, Pcrs as DriverPcrs};
use nsm_nitro_enclave_utils::time::Time;
use nsm_nitro_enclave_utils::verify::AttestationDocVerifierExt;
use proven_attestation::{AttestationParams, Attestor, Pcrs, VerifiedAttestation};
use rand::RngCore;
use serde_bytes::ByteBuf;

static END_CERT: &[u8] = include_bytes!("../mock_chain/end-certificate.der");
static INT_CERTS: &[&[u8]] = &[include_bytes!("../mock_chain/int-certificate.der")];
static ROOT_CERT: &[u8] = include_bytes!("../mock_chain/root-certificate.der");
static SIGNING_KEY: &[u8] = include_bytes!("../mock_chain/end-signing-key.der");

/// Noop attestation provider for local development.
#[derive(Clone)]
pub struct MockAttestor {
    driver: Arc<DevNitro>,
    pcrs: Pcrs,
}

impl MockAttestor {
    /// Create a new mock attestor.
    #[must_use]
    pub fn new() -> Self {
        let int_certs = INT_CERTS
            .iter()
            .map(|cert| ByteBuf::from(*cert))
            .collect::<Vec<ByteBuf>>();

        let end_cert = ByteBuf::from(END_CERT);

        let signing_key = SecretKey::from_pkcs8_der(&SIGNING_KEY).unwrap();

        let driver_pcrs = DriverPcrs::rand();

        let pcrs = Pcrs {
            pcr0: driver_pcrs.get(PcrIndex::Zero).to_vec().into(),
            pcr1: driver_pcrs.get(PcrIndex::One).to_vec().into(),
            pcr2: driver_pcrs.get(PcrIndex::Two).to_vec().into(),
            pcr3: driver_pcrs.get(PcrIndex::Three).to_vec().into(),
            pcr4: driver_pcrs.get(PcrIndex::Four).to_vec().into(),
            pcr8: driver_pcrs.get(PcrIndex::Eight).to_vec().into(),
        };

        let driver = nsm_nitro_enclave_utils::driver::dev::DevNitro::builder(signing_key, end_cert)
            .pcrs(driver_pcrs)
            .ca_bundle(int_certs)
            .build();

        Self {
            driver: Arc::new(driver),
            pcrs,
        }
    }
}

#[async_trait]
impl Attestor for MockAttestor {
    type Error = Error;

    async fn attest(&self, params: AttestationParams) -> Result<Bytes> {
        let attestation = self.driver.process_request(Request::Attestation {
            nonce: params.nonce.map(ByteBuf::from),
            public_key: params.public_key.map(ByteBuf::from),
            user_data: params.user_data.map(ByteBuf::from),
        });

        match attestation {
            Response::Attestation { document } => Ok(document.into()),
            _ => Err(Error::InvalidResponse),
        }
    }

    async fn pcrs(&self) -> Result<Pcrs> {
        Ok(self.pcrs.clone())
    }

    async fn secure_random(&self) -> Result<Bytes> {
        let mut rng = rand::rngs::OsRng;
        let mut random = vec![0; 32];
        rng.fill_bytes(&mut random);

        Ok(Bytes::from(random))
    }

    fn verify(&self, attestation: Bytes) -> Result<VerifiedAttestation> {
        let doc = AttestationDoc::from_cose(&attestation, ROOT_CERT, Time::default())?;

        Ok(VerifiedAttestation {
            nonce: doc.nonce.map(ByteBuf::into_vec).map(Bytes::from),
            pcrs: Pcrs {
                pcr0: doc.pcrs.get(&0).unwrap().to_vec().into(),
                pcr1: doc.pcrs.get(&1).unwrap().to_vec().into(),
                pcr2: doc.pcrs.get(&2).unwrap().to_vec().into(),
                pcr3: doc.pcrs.get(&3).unwrap().to_vec().into(),
                pcr4: doc.pcrs.get(&4).unwrap().to_vec().into(),
                pcr8: doc.pcrs.get(&8).unwrap().to_vec().into(),
            },
            public_key: doc.public_key.map(ByteBuf::into_vec).map(Bytes::from),
            user_data: doc.user_data.map(ByteBuf::into_vec).map(Bytes::from),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_attestation::AttestationParams;

    #[tokio::test]
    async fn test_roundtrip() {
        let attestor = MockAttestor::new();

        let nonce = Some(Bytes::from_static(b"test_nonce"));
        let public_key = Some(Bytes::from_static(b"test_public_key"));
        let user_data = Some(Bytes::from_static(b"test_user_data"));

        let params = AttestationParams {
            nonce: nonce.clone(),
            public_key: public_key.clone(),
            user_data: user_data.clone(),
        };

        let attestation_doc_bytes = attestor.attest(params).await.unwrap();

        let verified_attestation = attestor.verify(attestation_doc_bytes).unwrap();

        assert_eq!(verified_attestation.nonce, nonce);
        assert_eq!(verified_attestation.public_key, public_key);
        assert_eq!(verified_attestation.user_data, user_data);

        // Verify Pcrs separately as they are generated internally
        let expected_pcrs = attestor.pcrs().await.unwrap();
        assert_eq!(verified_attestation.pcrs.pcr0, expected_pcrs.pcr0);
        assert_eq!(verified_attestation.pcrs.pcr1, expected_pcrs.pcr1);
        assert_eq!(verified_attestation.pcrs.pcr2, expected_pcrs.pcr2);
        assert_eq!(verified_attestation.pcrs.pcr3, expected_pcrs.pcr3);
        assert_eq!(verified_attestation.pcrs.pcr4, expected_pcrs.pcr4);
        assert_eq!(verified_attestation.pcrs.pcr8, expected_pcrs.pcr8);
    }
}
