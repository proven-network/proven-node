use bytes::Bytes;
use coset::{CborSerializable, Label};
use ed25519_dalek::ed25519::signature::SignerMut;
use ed25519_dalek::{Signature, Verifier};
use proven_applications::ApplicationManagement;
use proven_code_package::CodePackage;
use proven_identity::Session;
use proven_runtime::{
    ExecutionRequest, ExecutionResult, HandlerSpecifier, ModuleLoader, RuntimePoolManagement,
};
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
    application_id: String,
    _application_manager: AM,
    runtime_pool_manager: RM,
    session: Session,
}

type Args = Vec<serde_json::Value>;
type HandlerSpecifierString = String;
type Module = String;
type ModuleHash = String;
#[repr(u8)]
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Request {
    Execute(Module, HandlerSpecifierString, Args),
    ExecuteHash(ModuleHash, HandlerSpecifierString, Args),
    Watch(String),
    WhoAmI,
}

#[derive(Debug, Serialize)]
pub enum Response {
    BadHandlerSpecifier,
    ExecuteHashUnknown,
    ExecuteSuccess(ExecutionResult),
    ExecuteFailure(String),
    Ok,
    WhoAmI(String),
}

impl<AM, RM> RpcHandler<AM, RM>
where
    AM: ApplicationManagement,
    RM: RuntimePoolManagement,
{
    pub fn new(
        application_manager: AM,
        runtime_pool_manager: RM,
        application_id: String,
        session: Session,
    ) -> Result<Self, RpcHandlerError> {
        Ok(Self {
            aad: session.session_id().as_bytes().to_vec(),
            application_id,
            _application_manager: application_manager,
            runtime_pool_manager,
            session,
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
                    .map(|signature| self.session.verifying_key().verify(pt, &signature))?
            })
            .map_err(|_| RpcHandlerError::Signature)?;

        let method: Request =
            ciborium::de::from_reader(payload.as_slice()).map_err(|_| RpcHandlerError::Payload)?;

        let response = match method {
            Request::Execute(module, handler_specifier_string, args) => {
                let execution_request = ExecutionRequest::Rpc {
                    application_id: self.application_id.clone(),
                    args,
                    handler_specifier: HandlerSpecifier::parse(&handler_specifier_string).unwrap(),
                    session: self.session.clone(),
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
                match HandlerSpecifier::parse(&handler_specifier_string) {
                    Ok(handler_specifier) => {
                        let execution_request = ExecutionRequest::Rpc {
                            application_id: self.application_id.clone(),
                            args,
                            handler_specifier,
                            session: self.session.clone(),
                        };

                        match self
                            .runtime_pool_manager
                            .execute_prehashed(code_package_hash, execution_request)
                            .await
                        {
                            Ok(result) => Ok(Response::ExecuteSuccess(result)),
                            Err(proven_runtime::Error::HashUnknown) => {
                                Ok(Response::ExecuteHashUnknown)
                            }
                            Err(e) => Ok(Response::ExecuteFailure(format!("{e:?}"))),
                        }
                    }
                    Err(_) => Ok(Response::BadHandlerSpecifier),
                }
            }
            Request::Watch(_) => Ok(Response::Ok),
            Request::WhoAmI => Ok(Response::WhoAmI(self.session.session_id().to_string())),
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
            .create_signature(&self.aad, |pt| {
                self.session.signing_key().clone().sign(pt).to_vec()
            })
            .build();

        resp_sign1
            .to_vec()
            .map_err(|_| RpcHandlerError::Sign1)
            .map(Bytes::from)
    }
}
