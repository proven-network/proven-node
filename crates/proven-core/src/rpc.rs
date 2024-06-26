pub mod http;
pub mod ws;

use coset::{CborSerializable, Label};
use ed25519_dalek::ed25519::signature::SignerMut;
use ed25519_dalek::{Signature, SigningKey, Verifier, VerifyingKey};
use proven_sessions::Session;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub enum RpcHandlerError {
    MethodNotFound,
    PayloadInvalid,
    SessionInvalid,
    Sign1Invalid,
    SignatureInvalid,
    SigningKeyInvalid,
    VerifyingKeyInvalid,
}

pub struct RpcHandler {
    aad: Vec<u8>,
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
    identity_address: String,
}

#[repr(u8)]
#[derive(Debug, Deserialize, Serialize)]
pub enum Request {
    WhoAmI = 0x0,
    Watch(String) = 0x1,
    Other,
}

#[derive(Debug, Serialize)]
pub enum Response {
    Ok,
    WhoAmI(WhoAmIResponse),
}

#[derive(Debug, Serialize)]
pub struct WhoAmIResponse {
    pub identity_address: String,
}

impl RpcHandler {
    pub fn new(session: Session) -> Result<Self, RpcHandlerError> {
        let signing_key_bytes: [u8; 32] = session
            .signing_key
            .try_into()
            .map_err(|_| RpcHandlerError::SigningKeyInvalid)?;
        let signing_key = SigningKey::from_bytes(&signing_key_bytes);

        let verifying_key_bytes: [u8; 32] = session
            .verifying_key
            .try_into()
            .map_err(|_| RpcHandlerError::VerifyingKeyInvalid)?;

        let verifying_key = VerifyingKey::from_bytes(&verifying_key_bytes)
            .map_err(|_| RpcHandlerError::VerifyingKeyInvalid)?;

        let aad = hex::decode(session.session_id).map_err(|_| RpcHandlerError::SessionInvalid)?;

        Ok(Self {
            aad,
            signing_key,
            verifying_key,
            identity_address: session.identity_address,
        })
    }

    pub async fn handle_rpc(&mut self, bytes: Vec<u8>) -> Result<Vec<u8>, RpcHandlerError> {
        let sign1 =
            coset::CoseSign1::from_slice(&bytes).map_err(|_| RpcHandlerError::Sign1Invalid)?;

        let payload = sign1
            .payload
            .as_ref()
            .ok_or(RpcHandlerError::Sign1Invalid)?;
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
                    .map(|signature| self.verifying_key.verify(pt, &signature))?
            })
            .map_err(|_| RpcHandlerError::SignatureInvalid)?;

        let method: Request =
            serde_cbor::from_slice(payload).map_err(|_| RpcHandlerError::PayloadInvalid)?;

        let response = match method {
            Request::WhoAmI => Ok(Response::WhoAmI(WhoAmIResponse {
                identity_address: self.identity_address.clone(),
            })),
            Request::Watch(_) => Ok(Response::Ok),
            _ => Err(RpcHandlerError::MethodNotFound),
        }?;

        let payload = serde_cbor::to_vec(&response).map_err(|_| RpcHandlerError::PayloadInvalid)?;

        let sign1_builder = match seq {
            None => coset::CoseSign1Builder::new(),
            Some(seq) => {
                let seq_header = coset::HeaderBuilder::new()
                    .text_value("seq".to_string(), seq)
                    .build();
                coset::CoseSign1Builder::new().unprotected(seq_header)
            }
        };

        let protected_header: coset::Header = coset::HeaderBuilder::new()
            .algorithm(coset::iana::Algorithm::EdDSA)
            .build();

        let resp_sign1 = sign1_builder
            .protected(protected_header)
            .payload(payload)
            .create_signature(&self.aad, |pt| self.signing_key.sign(pt).to_vec())
            .build();

        resp_sign1
            .to_vec()
            .map_err(|_| RpcHandlerError::Sign1Invalid)
    }
}
