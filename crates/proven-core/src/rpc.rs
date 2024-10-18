pub mod http;
pub mod ws;

use std::sync::Arc;

use coset::{CborSerializable, Label};
use ed25519_dalek::ed25519::signature::SignerMut;
use ed25519_dalek::{Signature, SigningKey, Verifier, VerifyingKey};
use proven_runtime::{Context, ExecutionRequest, ExecutionResult, Pool};
use proven_sessions::Session;
use serde::{Deserialize, Serialize};
use tracing::info;

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
    account_addresses: Vec<String>,
    runtime_pool: Arc<Pool>,
}

#[repr(u8)]
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub enum Request {
    WhoAmI = 0x0,
    Execute(String, String) = 0x1,
    Watch(String) = 0x2,
}

#[derive(Debug, Serialize)]
pub enum Response {
    Ok,
    ExecuteSuccess(ExecutionResult),
    ExecuteFailure,
    WhoAmI(WhoAmIResponse),
}

#[derive(Debug, Serialize)]
pub struct WhoAmIResponse {
    pub identity_address: String,
    pub account_addresses: Vec<String>,
}

impl RpcHandler {
    pub fn new(session: Session, runtime_pool: Arc<Pool>) -> Result<Self, RpcHandlerError> {
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
            account_addresses: session.account_addresses,
            runtime_pool,
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

        let payload_hex = hex::encode(payload);
        println!("Payload: {:?}", payload_hex);

        let method: Request =
            serde_cbor::from_slice(payload).map_err(|_| RpcHandlerError::PayloadInvalid)?;

        let response = match method {
            Request::WhoAmI => Ok(Response::WhoAmI(WhoAmIResponse {
                identity_address: self.identity_address.clone(),
                account_addresses: self.account_addresses.clone(),
            })),
            Request::Execute(module, handler_name) => {
                let pool = Arc::clone(&self.runtime_pool);

                let request = ExecutionRequest {
                    context: Context {
                        identity: Some(self.identity_address.clone()),
                        accounts: Some(vec![]),
                    },
                    handler_name,
                    args: vec![],
                };

                match pool.execute(module, request).await {
                    Ok(result) => {
                        info!("Successful execution: {:?}", result);
                        Ok(Response::ExecuteSuccess(result))
                    }
                    Err(_) => Ok(Response::ExecuteFailure),
                }
            }
            Request::Watch(_) => Ok(Response::Ok),
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
