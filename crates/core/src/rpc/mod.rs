mod auth;
mod commands;
mod context;
mod error;

use bytes::Bytes;
use proven_applications::ApplicationManagement;
use proven_identity::IdentityManagement;
use proven_runtime::RuntimePoolManagement;
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

#[derive(Debug, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum Command {
    Anonymize(AnonymizeCommand),
    CreateApplication(CreateApplicationCommand),
    Execute(ExecuteCommand),
    ExecuteHash(ExecuteHashCommand),
    Identify(IdentifyCommand),
    WhoAmI(WhoAmICommand),
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", content = "data")]
pub enum Response {
    Anonymize(AnonymizeResponse),
    CreateApplication(CreateApplicationResponse),
    Execute(ExecuteResponse),
    ExecuteHash(ExecuteHashResponse),
    Identify(IdentifyResponse),
    WhoAmI(WhoAmIResponse),
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
