use crate::session::Session;

use serde::Serialize;
use uuid::Uuid;

/// Type returned to the client to identify the session which strips sensitive data (e.g. signing keys).
/// A session.
#[derive(Clone, Debug, Serialize)]
pub enum WhoAmI {
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
        identity_id: Uuid,

        /// The origin of the session.
        origin: String,

        /// The session ID.
        session_id: String,
    },
}

impl From<Session> for WhoAmI {
    fn from(session: Session) -> Self {
        match session {
            Session::Anonymous {
                origin, session_id, ..
            } => WhoAmI::Anonymous {
                origin,
                session_id: session_id.to_string(),
            },
            Session::Identified {
                identity_id,
                origin,
                session_id,
                ..
            } => WhoAmI::Identified {
                identity_id,
                origin,
                session_id: session_id.to_string(),
            },
        }
    }
}
