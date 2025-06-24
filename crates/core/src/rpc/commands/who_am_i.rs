use crate::rpc::commands::RpcCommand;
use crate::rpc::context::RpcContext;

use async_trait::async_trait;
use proven_sessions::Session;
use serde::{Deserialize, Serialize};

/// Type returned to the client to identify the session which strips sensitive data (e.g. signing keys).
#[derive(Clone, Debug, Serialize)]
pub enum WhoAmIResponse {
    /// An anonymous session - yet to be identified via a ledger ID handshake.
    Anonymous {
        /// The origin of the session.
        origin: String,

        /// The session ID.
        session_id: String,
    },

    /// An identified session - identified via a ledger ID handshake.
    Identified {
        /// The Proven identity ID.
        identity_id: String,

        /// The origin of the session.
        origin: String,

        /// The session ID.
        session_id: String,
    },
}

impl From<Session> for WhoAmIResponse {
    fn from(session: Session) -> Self {
        match session {
            Session::Anonymous {
                origin, session_id, ..
            } => WhoAmIResponse::Anonymous {
                origin,
                session_id: session_id.to_string(),
            },
            Session::Identified {
                identity_id,
                origin,
                session_id,
                ..
            } => WhoAmIResponse::Identified {
                identity_id: identity_id.to_string(),
                origin,
                session_id: session_id.to_string(),
            },
        }
    }
}

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
        context.session.clone().into()
    }
}
