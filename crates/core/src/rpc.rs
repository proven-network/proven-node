pub mod http;
pub mod ws;

use bytes::Bytes;
use coset::{CborSerializable, Label};
use ed25519_dalek::ed25519::signature::SignerMut;
use ed25519_dalek::{Signature, SigningKey, Verifier, VerifyingKey};
use proven_applications::{Application, ApplicationManagement, CreateApplicationOptions};
use proven_code_package::CodePackage;
use proven_runtime::{
    ExecutionRequest, ExecutionResult, HandlerSpecifier, ModuleLoader, RuntimePoolManagement,
};
use proven_sessions::Session;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors that can occur during RPC.
#[derive(Debug, Error)]
pub enum RpcHandlerError {
    /// Invalid payload.
    #[error("Invalid payload")]
    Payload,

    /// Invalid session.
    #[error("Invalid session")]
    Session,

    /// Invalid COSE Sign1.
    #[error("Invalid COSE_Sign1")]
    Sign1,

    /// Invalid signature.
    #[error("Invalid signature")]
    Signature,

    /// Invalid signing key.
    #[error("Invalid signing key")]
    SigningKey,

    /// Invalid verifying key.
    #[error("Invalid verifying key")]
    VerifyingKey,
}

pub struct RpcHandler<AM, RM>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
{
    aad: Vec<u8>,
    account_addresses: Vec<String>,
    application_manager: AM,
    dapp_definition_address: String,
    identity_address: String,
    runtime_pool_manager: RM,
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
}

type Args = Vec<serde_json::Value>;
type HandlerSpecifierString = String;
type Module = String;
type ModuleHash = String;
#[repr(u8)]
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Request {
    CreateApplication,
    Execute(Module, HandlerSpecifierString, Args),
    ExecuteHash(ModuleHash, HandlerSpecifierString, Args),
    Watch(String),
    WhoAmI,
}

#[derive(Debug, Serialize)]
pub enum Response {
    CreateApplicationFailure(String),
    CreateApplicationSuccess(Application),
    ExecuteHashUnknown,
    ExecuteSuccess(ExecutionResult),
    ExecuteFailure(String),
    Ok,
    WhoAmI(WhoAmIResponse),
}

#[derive(Debug, Serialize)]
pub struct WhoAmIResponse {
    pub identity_address: String,
    pub account_addresses: Vec<String>,
}

impl<AM, RM> RpcHandler<AM, RM>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
{
    pub fn new(
        application_manager: AM,
        runtime_pool_manager: RM,
        session: Session,
    ) -> Result<Self, RpcHandlerError> {
        let signing_key_bytes: [u8; 32] = session
            .signing_key
            .try_into()
            .map_err(|_| RpcHandlerError::SigningKey)?;
        let signing_key = SigningKey::from_bytes(&signing_key_bytes);

        let verifying_key_bytes: [u8; 32] = session
            .verifying_key
            .try_into()
            .map_err(|_| RpcHandlerError::VerifyingKey)?;

        let verifying_key = VerifyingKey::from_bytes(&verifying_key_bytes)
            .map_err(|_| RpcHandlerError::VerifyingKey)?;

        let aad = hex::decode(session.session_id).map_err(|_| RpcHandlerError::Session)?;

        Ok(Self {
            aad,
            account_addresses: session.account_addresses,
            application_manager,
            dapp_definition_address: session.dapp_definition_address,
            identity_address: session.identity_address,
            runtime_pool_manager,
            signing_key,
            verifying_key,
        })
    }

    #[allow(clippy::too_many_lines)]
    pub async fn handle_rpc(&mut self, bytes: Bytes) -> Result<Bytes, RpcHandlerError> {
        let sign1 = coset::CoseSign1::from_slice(&bytes).map_err(|_| RpcHandlerError::Sign1)?;

        let payload = sign1.payload.as_ref().ok_or(RpcHandlerError::Sign1)?;
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
            .map_err(|_| RpcHandlerError::Signature)?;

        let method: Request =
            ciborium::de::from_reader(payload.as_slice()).map_err(|_| RpcHandlerError::Payload)?;

        let response = match method {
            Request::CreateApplication => {
                match self
                    .application_manager
                    .create_application(CreateApplicationOptions {
                        owner_identity_address: self.identity_address.clone(),
                        dapp_definition_addresses: vec![],
                    })
                    .await
                {
                    Ok(application) => Ok(Response::CreateApplicationSuccess(application)),
                    Err(e) => Ok(Response::CreateApplicationFailure(e.to_string())),
                }
            }
            Request::Execute(module, handler_specifier_string, args) => {
                let execution_request = ExecutionRequest::Rpc {
                    args,
                    accounts: self.account_addresses.clone(),
                    handler_specifier: HandlerSpecifier::parse(&handler_specifier_string).unwrap(),
                    dapp_definition_address: self.dapp_definition_address.clone(),
                    identity: self.identity_address.clone(),
                };

                match self
                    .runtime_pool_manager
                    .execute(
                        ModuleLoader::new(CodePackage::from_str(&module).unwrap()),
                        execution_request,
                    )
                    .await
                {
                    Ok(result) => Ok(Response::ExecuteSuccess(result)),
                    Err(e) => Ok(Response::ExecuteFailure(format!("{e:?}"))),
                }
            }
            Request::ExecuteHash(code_package_hash, handler_specifier_string, args) => {
                let execution_request = ExecutionRequest::Rpc {
                    accounts: self.account_addresses.clone(),
                    args,
                    dapp_definition_address: self.dapp_definition_address.clone(),
                    handler_specifier: HandlerSpecifier::parse(&handler_specifier_string).unwrap(),
                    identity: self.identity_address.clone(),
                };

                match self
                    .runtime_pool_manager
                    .execute_prehashed(code_package_hash, execution_request)
                    .await
                {
                    Ok(result) => Ok(Response::ExecuteSuccess(result)),
                    Err(proven_runtime::Error::HashUnknown) => Ok(Response::ExecuteHashUnknown),
                    Err(e) => Ok(Response::ExecuteFailure(format!("{e:?}"))),
                }
            }
            Request::Watch(_) => Ok(Response::Ok),
            Request::WhoAmI => Ok(Response::WhoAmI(WhoAmIResponse {
                identity_address: self.identity_address.clone(),
                account_addresses: self.account_addresses.clone(),
            })),
        }?;

        let mut payload = Vec::new();
        ciborium::ser::into_writer(&response, &mut payload)
            .map_err(|_| RpcHandlerError::Payload)?;

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
            .payload(payload)
            .create_signature(&self.aad, |pt| self.signing_key.sign(pt).to_vec())
            .build();

        resp_sign1
            .to_vec()
            .map_err(|_| RpcHandlerError::Sign1)
            .map(Bytes::from)
    }
}
