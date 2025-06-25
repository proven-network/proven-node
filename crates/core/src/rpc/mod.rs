mod auth;
mod commands;
mod context;
mod error;

use bytes::Bytes;
use proven_applications::{Application, ApplicationManagement};
use proven_identity::{Identity, IdentityManagement};
use proven_runtime::{ExecutionResult, RuntimePoolManagement};
use proven_sessions::{Session, SessionManagement};
use serde::{Deserialize, Serialize};

pub use auth::RpcAuth;
pub use commands::RpcCommand;
pub use context::RpcContext;
pub use error::Error;
use uuid::Uuid;

use crate::rpc::commands::{
    AnonymizeCommand, AnonymizeResponse, CreateApplicationCommand, CreateApplicationResponse,
    ExecuteCommand, ExecuteHashCommand, ExecuteHashResponse, ExecuteResponse, IdentifyCommand,
    IdentifyResponse, WhoAmICommand, WhoAmIResponse,
};

#[repr(u8)]
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Request {
    // no args
    Anonymize,
    // no args
    CreateApplication,
    // module, handler_specifier, args
    Execute(String, String, Vec<serde_json::Value>),
    // module_hash, handler_specifier, args
    ExecuteHash(String, String, Vec<serde_json::Value>),
    // passkey_prf_public_key, session_id_signature
    Identify(Bytes, Bytes),
    // no args
    WhoAmI,
}

#[derive(Debug)]
pub enum Command {
    Anonymize(AnonymizeCommand),
    CreateApplication(CreateApplicationCommand),
    Execute(ExecuteCommand),
    ExecuteHash(ExecuteHashCommand),
    Identify(IdentifyCommand),
    WhoAmI(WhoAmICommand),
}

#[async_trait::async_trait]
impl RpcCommand for Command {
    type Response = Response;

    async fn execute<AM, IM, SM, RM>(&self, context: &mut RpcContext<AM, IM, SM, RM>) -> Response
    where
        AM: ApplicationManagement,
        IM: IdentityManagement,
        SM: SessionManagement,
        RM: RuntimePoolManagement,
    {
        match self {
            Command::Anonymize(cmd) => {
                let r = cmd.execute(context).await;
                match r {
                    AnonymizeResponse::AnonymizeFailure(err) => Response::AnonymizeFailure(err),
                    AnonymizeResponse::AnonymizeSuccess => Response::AnonymizeSuccess,
                }
            }
            Command::CreateApplication(cmd) => {
                let r = cmd.execute(context).await;
                match r {
                    CreateApplicationResponse::CreateApplicationFailure(err) => {
                        Response::CreateApplicationFailure(err)
                    }
                    CreateApplicationResponse::CreateApplicationSuccess(application) => {
                        Response::CreateApplicationSuccess(application)
                    }
                }
            }
            Command::Execute(cmd) => {
                let r = cmd.execute(context).await;
                match r {
                    ExecuteResponse::BadHandlerSpecifier => Response::BadHandlerSpecifier,
                    ExecuteResponse::Failure(err) => Response::ExecuteFailure(err),
                    ExecuteResponse::Success(result) => Response::ExecuteSuccess(result),
                }
            }
            Command::ExecuteHash(cmd) => {
                let r = cmd.execute(context).await;
                match r {
                    ExecuteHashResponse::BadHandlerSpecifier => Response::BadHandlerSpecifier,
                    ExecuteHashResponse::Failure(err) => Response::ExecuteFailure(err),
                    ExecuteHashResponse::HashUnknown => Response::ExecuteHashUnknown,
                    ExecuteHashResponse::Success(result) => Response::ExecuteSuccess(result),
                }
            }
            Command::Identify(cmd) => {
                let r = cmd.execute(context).await;
                match r {
                    IdentifyResponse::IdentifyFailure(err) => Response::IdentifyFailure(err),
                    IdentifyResponse::IdentifySuccess(identity) => {
                        Response::IdentifySuccess(identity)
                    }
                }
            }
            Command::WhoAmI(cmd) => Response::WhoAmI(cmd.execute(context).await),
        }
    }
}

impl Request {
    pub fn into_command(self) -> Command {
        match self {
            Request::Anonymize => Command::Anonymize(AnonymizeCommand),
            Request::CreateApplication => Command::CreateApplication(CreateApplicationCommand),
            Request::Execute(module, handler_specifier, args) => Command::Execute(ExecuteCommand {
                args,
                handler_specifier,
                module,
            }),
            Request::ExecuteHash(module_hash, handler_specifier, args) => {
                Command::ExecuteHash(ExecuteHashCommand {
                    args,
                    handler_specifier,
                    module_hash,
                })
            }
            Request::Identify(passkey_prf_public_key_bytes, session_id_signature_bytes) => {
                Command::Identify(IdentifyCommand {
                    passkey_prf_public_key_bytes,
                    session_id_signature_bytes,
                })
            }
            Request::WhoAmI => Command::WhoAmI(WhoAmICommand),
        }
    }
}

#[derive(Debug, Serialize)]
pub enum Response {
    AnonymizeFailure(String),
    AnonymizeSuccess,
    BadHandlerSpecifier,
    CreateApplicationFailure(String),
    CreateApplicationSuccess(Application),
    ExecuteFailure(String),
    ExecuteHashUnknown,
    ExecuteSuccess(ExecutionResult),
    IdentifyFailure(String),
    // TODO: strip this down to something client-safe
    IdentifySuccess(Identity),
    WhoAmI(WhoAmIResponse),
}

/// Main RPC handler that coordinates authentication and command execution
pub struct RpcHandler<AM, IM, SM, RM>
where
    AM: ApplicationManagement,
    IM: IdentityManagement,
    SM: SessionManagement,
    RM: RuntimePoolManagement,
{
    auth: RpcAuth,
    context: RpcContext<AM, IM, SM, RM>,
}

impl<AM, IM, SM, RM> RpcHandler<AM, IM, SM, RM>
where
    AM: ApplicationManagement,
    IM: IdentityManagement,
    SM: SessionManagement,
    RM: RuntimePoolManagement,
{
    pub fn new(
        application_id: Uuid,
        application_manager: AM,
        runtime_pool_manager: RM,
        identity_manager: IM,
        sessions_manager: SM,
        session: Session,
    ) -> Self {
        let auth = RpcAuth::new(session.clone());
        let context = RpcContext::new(
            application_id,
            application_manager,
            identity_manager,
            sessions_manager,
            runtime_pool_manager,
            session,
        );

        Self { auth, context }
    }

    pub async fn handle_rpc(&mut self, bytes: Bytes) -> Result<Bytes, Error> {
        // Verify the incoming request
        let (payload, seq) = self.auth.verify(&bytes)?;

        // Deserialize the request
        let request: Request =
            ciborium::de::from_reader(&payload[..]).map_err(|_| Error::Deserialize)?;

        // Execute the command and convert to response
        let response = request.into_command().execute(&mut self.context).await;

        // Serialize the response
        let mut payload = Vec::new();
        ciborium::ser::into_writer(&response, &mut payload).map_err(|_| Error::Serialize)?;

        // Sign and return the response
        self.auth.sign(&payload, seq)
    }
}
