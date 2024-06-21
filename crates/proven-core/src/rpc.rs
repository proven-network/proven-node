use coset::{CborSerializable, Label};
use ed25519_dalek::ed25519::signature::SignerMut;
use ed25519_dalek::{Signature, SigningKey, Verifier, VerifyingKey};
use proven_sessions::Session;
use serde::Deserialize;

#[derive(Debug)]
pub enum RpcHandlerError {
    MethodNotFound,
    Sign1Invalid,
    SignatureInvalid,
    SigningKeyInvalid,
    VerifyingKeyInvalid,
}

pub struct RpcHandler {
    session_id: String,
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
    identity_address: String,
}

#[repr(u8)]
#[derive(Debug, Deserialize)]
pub enum Method {
    WhoAmI = 0x0,
    Watch(String) = 0x1,
}

const SEQ_HEADER: Label = Label::Int(42);

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

        Ok(Self {
            session_id: session.session_id,
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
            .find(|(l, _)| l == &SEQ_HEADER)
            .map(|(_, v)| v);

        sign1
            .verify_signature(self.session_id.as_bytes(), |signature_bytes, pt| {
                Signature::from_slice(signature_bytes)
                    .map(|signature| self.verifying_key.verify(pt, &signature))?
            })
            .map_err(|_| RpcHandlerError::SignatureInvalid)?;

        let method: Method =
            serde_cbor::from_slice(payload).map_err(|_| RpcHandlerError::Sign1Invalid)?;

        let response = match method {
            Method::WhoAmI => Ok(self.identity_address.as_bytes().to_vec()),
            _ => Err(RpcHandlerError::MethodNotFound),
        }?;

        let protected_header: coset::Header = coset::HeaderBuilder::new()
            .algorithm(coset::iana::Algorithm::EdDSA)
            .build();

        let sign1_builder = match seq {
            None => coset::CoseSign1Builder::new(),
            Some(seq) => {
                let seq_header = coset::HeaderBuilder::new().value(42, seq).build();
                coset::CoseSign1Builder::new().unprotected(seq_header)
            }
        };

        let resp_sign1 = sign1_builder
            .protected(protected_header)
            .payload(response)
            .create_signature(self.session_id.as_bytes(), |pt| {
                self.signing_key.sign(pt).to_vec()
            })
            .build();

        resp_sign1
            .to_vec()
            .map_err(|_| RpcHandlerError::Sign1Invalid)
    }
}
