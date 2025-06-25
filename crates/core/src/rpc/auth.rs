use bytes::Bytes;
use coset::{CborSerializable, Label, cbor::Value};
use ed25519_dalek::ed25519::signature::SignerMut;
use ed25519_dalek::{Signature, Verifier};
use proven_sessions::Session;

use crate::rpc::error::Error;

/// Handles all COSE signing and verification for RPC messages
pub struct RpcAuth {
    aad: Vec<u8>,
    session: Session,
}

impl RpcAuth {
    pub fn new(session: Session) -> Self {
        Self {
            aad: session.session_id().as_bytes().to_vec(),
            session,
        }
    }

    /// Verify and extract the payload from a signed request
    pub fn verify(&self, bytes: &[u8]) -> Result<(Bytes, Option<Value>), Error> {
        let sign1 = coset::CoseSign1::from_slice(bytes).map_err(|_| Error::Sign1)?;

        let payload = sign1.payload.as_ref().ok_or(Error::Sign1)?;
        let seq = sign1
            .unprotected
            .rest
            .clone()
            .into_iter()
            .find(|(l, _)| l == &Label::Text("seq".to_string()))
            .map(|(_, v)| v);

        sign1
            .verify_signature(&self.aad, |signature_bytes, pt| {
                Signature::from_slice(signature_bytes)
                    .map(|signature| self.session.verifying_key().verify(pt, &signature))?
            })
            .map_err(|_| Error::Signature)?;

        Ok((Bytes::copy_from_slice(payload), seq))
    }

    /// Sign a response payload
    pub fn sign(&self, payload: &[u8], seq: Option<Value>) -> Result<Bytes, Error> {
        let sign1_builder = seq.map_or_else(coset::CoseSign1Builder::new, |seq| {
            let seq_header = coset::HeaderBuilder::new()
                .text_value("seq".to_string(), seq)
                .build();
            coset::CoseSign1Builder::new().unprotected(seq_header)
        });

        let protected_header: coset::Header = coset::HeaderBuilder::new()
            .algorithm(coset::iana::Algorithm::EdDSA)
            .build();

        let resp_sign1 = sign1_builder
            .protected(protected_header)
            .payload(payload.to_vec())
            .create_signature(&self.aad, |pt| {
                self.session.signing_key().clone().sign(pt).to_vec()
            })
            .build();

        resp_sign1
            .to_vec()
            .map_err(|_| Error::Sign1)
            .map(Bytes::from)
    }
}
