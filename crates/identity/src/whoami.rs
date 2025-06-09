use crate::identity::Identity;
use crate::ledger_identity::LedgerIdentity;
use crate::session::Session;

use serde::Serialize;

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
        /// The Proven identity.
        identity: Identity,

        /// The active ledger identity.
        ledger_identity: LedgerIdentity,

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
                identity,
                ledger_identity,
                origin,
                session_id,
                ..
            } => WhoAmI::Identified {
                identity,
                ledger_identity,
                origin,
                session_id: session_id.to_string(),
            },
        }
    }
}
