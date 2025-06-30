use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

use async_trait::async_trait;
use proven_identity::Identity;
use proven_sessions::{ApplicationSession, ManagementSession, Session};
use serde::{Deserialize, Serialize};

/// Type returned to the client to identify the session which strips sensitive data (e.g. signing keys).
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "result", content = "data")]
pub enum WhoAmIResponse {
    /// An anonymous session - yet to be identified via a ledger ID handshake.
    #[serde(rename = "anonymous")]
    Anonymous {
        /// The origin of the session.
        origin: String,

        /// The session ID.
        session_id: String,
    },

    /// A failure to get the identity of the current session.
    #[serde(rename = "failure")]
    Failure(String),

    /// An identified session - identified via a ledger ID handshake.
    #[serde(rename = "identified")]
    Identified {
        /// The Proven identity.
        identity: Identity,

        /// The origin of the session.
        origin: String,

        /// The session ID.
        session_id: String,
    },
}

/// Command to get the identity of the current session.
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WhoAmICommand;

#[async_trait]
impl RpcCommand for WhoAmICommand {
    type Response = WhoAmIResponse;

    async fn execute<AM, IM, SM, RM>(
        &self,
        context: &mut RpcContext<AM, IM, SM, RM>,
    ) -> Self::Response
    where
        AM: proven_applications::ApplicationManagement,
        IM: proven_identity::IdentityManagement,
        SM: proven_sessions::SessionManagement,
        RM: proven_runtime::RuntimePoolManagement,
    {
        match &context.session {
            Session::Application(app_session) => match app_session {
                ApplicationSession::Anonymous {
                    origin, session_id, ..
                } => WhoAmIResponse::Anonymous {
                    origin: origin.clone(),
                    session_id: session_id.to_string(),
                },
                ApplicationSession::Identified {
                    identity_id,
                    origin,
                    session_id,
                    ..
                } => match context.identity_manager.get_identity(identity_id).await {
                    Ok(Some(identity)) => WhoAmIResponse::Identified {
                        identity,
                        origin: origin.clone(),
                        session_id: session_id.to_string(),
                    },
                    Ok(None) => WhoAmIResponse::Failure("Identity not found".to_string()),
                    Err(e) => WhoAmIResponse::Failure(e.to_string()),
                },
            },
            Session::Management(mgmt_session) => match mgmt_session {
                ManagementSession::Anonymous {
                    origin, session_id, ..
                } => WhoAmIResponse::Anonymous {
                    origin: origin.clone(),
                    session_id: session_id.to_string(),
                },
                ManagementSession::Identified {
                    identity_id,
                    origin,
                    session_id,
                    ..
                } => match context.identity_manager.get_identity(identity_id).await {
                    Ok(Some(identity)) => WhoAmIResponse::Identified {
                        identity,
                        origin: origin.clone(),
                        session_id: session_id.to_string(),
                    },
                    Ok(None) => WhoAmIResponse::Failure("Identity not found".to_string()),
                    Err(e) => WhoAmIResponse::Failure(e.to_string()),
                },
            },
        }
    }
}
