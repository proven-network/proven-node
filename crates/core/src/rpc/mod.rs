mod auth;
mod commands;
mod context;
mod error;

pub use auth::RpcAuth;
pub use context::RpcContext;
pub use error::Error;

pub use crate::rpc::commands::{
    AnonymizeCommand, AnonymizeResponse, CreateApplicationCommand, CreateApplicationResponse,
    ExecuteCommand, ExecuteHashCommand, ExecuteHashResponse, ExecuteResponse, IdentifyCommand,
    IdentifyResponse, WhoAmICommand, WhoAmIResponse,
};

use async_trait::async_trait;
use bytes::Bytes;
use commands::RpcCommand;
use proven_applications::ApplicationManagement;
use proven_identity::IdentityManagement;
use proven_runtime::RuntimePoolManagement;
use proven_sessions::{Session, SessionManagement};
use serde::{Deserialize, Serialize};

/// A command to be sent to the RPC server.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "data")]
pub enum Command {
    /// Anonymize the session.
    Anonymize(AnonymizeCommand),

    /// Create an application.
    CreateApplication(CreateApplicationCommand),

    /// Execute a code hash.
    ExecuteHash(ExecuteHashCommand),

    /// Execute a code.
    Execute(ExecuteCommand),

    /// Identify the session.
    Identify(IdentifyCommand),

    /// Get the identity of the current session.
    WhoAmI(WhoAmICommand),
}

/// A response to a command sent to the RPC server.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "data")]
pub enum Response {
    /// A response to the anonymize command.
    Anonymize(AnonymizeResponse),

    /// A response to the create application command.
    CreateApplication(CreateApplicationResponse),

    /// A response to the execute command.
    Execute(ExecuteResponse),

    /// A response to the execute hash command.
    ExecuteHash(ExecuteHashResponse),

    /// A response to the identify command.
    Identify(IdentifyResponse),

    /// A response to the who am i command.
    WhoAmI(WhoAmIResponse),
}

#[async_trait]
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
            Command::Anonymize(cmd) => Response::Anonymize(cmd.execute(context).await),
            Command::CreateApplication(cmd) => {
                Response::CreateApplication(cmd.execute(context).await)
            }
            Command::Execute(cmd) => Response::Execute(cmd.execute(context).await),
            Command::ExecuteHash(cmd) => Response::ExecuteHash(cmd.execute(context).await),
            Command::Identify(cmd) => Response::Identify(cmd.execute(context).await),
            Command::WhoAmI(cmd) => Response::WhoAmI(cmd.execute(context).await),
        }
    }
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
        application_manager: AM,
        identity_manager: IM,
        sessions_manager: SM,
        runtime_pool_manager: RM,
        session: Session,
    ) -> Self {
        let auth = RpcAuth::new(session.clone());
        let context = RpcContext::new(
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

        // Deserialize directly into Command
        let command: Command =
            ciborium::de::from_reader(&payload[..]).map_err(|_| Error::Deserialize)?;

        // Execute the command and convert to response
        let response = command.execute(&mut self.context).await;

        // Serialize the response
        let mut payload = Vec::new();
        ciborium::ser::into_writer(&response, &mut payload).map_err(|_| Error::Serialize)?;

        // Sign and return the response
        self.auth.sign(&payload, seq)
    }
}
