mod auth;
mod commands;
mod context;
mod error;

use bytes::Bytes;
use proven_applications::ApplicationManagement;
use proven_identity::{Identity, IdentityManagement, Session, WhoAmI};
use proven_runtime::{ExecutionResult, RuntimePoolManagement};
use serde::{Deserialize, Serialize};

pub use auth::RpcAuth;
pub use commands::RpcCommand;
pub use context::RpcContext;
pub use error::Error;

use crate::rpc::commands::{
    ExecuteCommand, ExecuteHashCommand, ExecuteHashResponse, ExecuteResponse, IdentifyCommand,
    IdentifyResponse, WhoAmICommand,
};

#[repr(u8)]
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Request {
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
    Execute(ExecuteCommand),
    ExecuteHash(ExecuteHashCommand),
    Identify(IdentifyCommand),
    WhoAmI(WhoAmICommand),
}

#[async_trait::async_trait]
impl RpcCommand for Command {
    type Response = Response;

    async fn execute<AM, IM, RM>(&self, context: &mut RpcContext<AM, IM, RM>) -> Response
    where
        AM: ApplicationManagement,
        IM: IdentityManagement,
        RM: RuntimePoolManagement,
    {
        match self {
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
            Request::Execute(module, handler_specifier, args) => Command::Execute(ExecuteCommand {
                module,
                handler_specifier,
                args,
            }),
            Request::ExecuteHash(module_hash, handler_specifier, args) => {
                Command::ExecuteHash(ExecuteHashCommand {
                    module_hash,
                    handler_specifier,
                    args,
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
    BadHandlerSpecifier,
    ExecuteFailure(String),
    ExecuteHashUnknown,
    ExecuteSuccess(ExecutionResult),
    IdentifyFailure(String),
    // TODO: strip this down to something client-safe
    IdentifySuccess(Identity),
    WhoAmI(WhoAmI),
}

/// Main RPC handler that coordinates authentication and command execution
pub struct RpcHandler<AM, IM, RM>
where
    AM: ApplicationManagement,
    IM: IdentityManagement,
    RM: RuntimePoolManagement,
{
    auth: RpcAuth,
    context: RpcContext<AM, IM, RM>,
}

impl<AM, IM, RM> RpcHandler<AM, IM, RM>
where
    AM: ApplicationManagement,
    IM: IdentityManagement,
    RM: RuntimePoolManagement,
{
    pub fn new(
        application_manager: AM,
        runtime_pool_manager: RM,
        identity_manager: IM,
        application_id: String,
        session: Session,
    ) -> Result<Self, Error> {
        let auth = RpcAuth::new(session.clone())?;
        let context = RpcContext::new(
            application_id,
            application_manager,
            identity_manager,
            runtime_pool_manager,
            session,
        );

        Ok(Self { auth, context })
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
