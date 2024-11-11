pub mod http;
pub mod ws;

use std::sync::Arc;

use coset::{CborSerializable, Label};
use ed25519_dalek::ed25519::signature::SignerMut;
use ed25519_dalek::{Signature, SigningKey, Verifier, VerifyingKey};
use proven_runtime::{ExecutionRequest, ExecutionResult, Pool, PoolRuntimeOptions};
use proven_sessions::Session;
use proven_store::{Store2, Store3};
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

pub struct RpcHandler<AS: Store2, PS: Store3, NS: Store3> {
    aad: Vec<u8>,
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
    dapp_definition_address: String,
    identity_address: String,
    account_addresses: Vec<String>,
    runtime_pool: Arc<Pool<AS, PS, NS>>,
}

type Args = Vec<serde_json::Value>;
type HandlerName = String;
type Module = String;
type ModuleHash = String;
#[repr(u8)]
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub enum Request {
    WhoAmI = 0x0,
    Execute(Module, HandlerName, Args) = 0x1,
    ExecuteHash(ModuleHash, Args) = 0x2,
    Watch(String) = 0x3,
}

#[derive(Debug, Serialize)]
pub enum Response {
    Ok,
    ExecuteHashUnknown,
    ExecuteSuccess(ExecutionResult),
    ExecuteFailure(String),
    WhoAmI(WhoAmIResponse),
}

#[derive(Debug, Serialize)]
pub struct WhoAmIResponse {
    pub identity_address: String,
    pub account_addresses: Vec<String>,
}

impl<AS: Store2, PS: Store3, NS: Store3> RpcHandler<AS, PS, NS> {
    pub fn new(
        session: Session,
        runtime_pool: Arc<Pool<AS, PS, NS>>,
    ) -> Result<Self, RpcHandlerError> {
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
            dapp_definition_address: session.dapp_definition_address,
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

        let method: Request =
            serde_cbor::from_slice(payload).map_err(|_| RpcHandlerError::PayloadInvalid)?;

        let response = match method {
            Request::WhoAmI => Ok(Response::WhoAmI(WhoAmIResponse {
                identity_address: self.identity_address.clone(),
                account_addresses: self.account_addresses.clone(),
            })),
            Request::ExecuteHash(options_hash, args) => {
                let pool = Arc::clone(&self.runtime_pool);

                let request = ExecutionRequest {
                    accounts: Some(self.account_addresses.clone()),
                    args,
                    dapp_definition_address: self.dapp_definition_address.clone(),
                    identity: Some(self.identity_address.clone()),
                };

                match pool.execute_prehashed(options_hash, request).await {
                    Ok(result) => Ok(Response::ExecuteSuccess(result)),
                    Err(proven_runtime::Error::HashUnknown) => Ok(Response::ExecuteHashUnknown),
                    Err(e) => Ok(Response::ExecuteFailure(format!("{:?}", e))),
                }
            }
            Request::Execute(module, handler_name, args) => {
                let pool = Arc::clone(&self.runtime_pool);

                let request = ExecutionRequest {
                    args,
                    accounts: Some(self.account_addresses.clone()),
                    dapp_definition_address: self.dapp_definition_address.clone(),
                    identity: Some(self.identity_address.clone()),
                };

                match pool
                    .execute(
                        PoolRuntimeOptions {
                            handler_name: Some(handler_name.clone()),
                            module,
                        },
                        request,
                    )
                    .await
                {
                    Ok(result) => Ok(Response::ExecuteSuccess(result)),
                    Err(e) => Ok(Response::ExecuteFailure(format!("{:?}", e))),
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