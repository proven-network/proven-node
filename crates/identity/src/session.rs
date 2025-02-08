use crate::identity::Identity;
use crate::ledger_identity::LedgerIdentity;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// A session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Session {
    Anonymous {
        /// The origin of the session.
        origin: String,

        /// The session ID.
        session_id: String,

        /// The Ed25519 signing key for the server-side generated key.
        signing_key: Vec<u8>,

        /// The Ed25519 verifying key for the client-side generated key.
        verifying_key: Vec<u8>,
    },

    Identified {
        /// The Proven identity.
        identity: Identity,

        /// The active ledger identity.
        ledger_identity: LedgerIdentity,

        /// The origin of the session.
        origin: String,

        /// The session ID.
        session_id: String,

        /// The Ed25519 signing key for the server-side generated key.
        signing_key: Vec<u8>,

        /// The Ed25519 verifying key for the client-side generated key.
        verifying_key: Vec<u8>,
    },
}

impl Session {
    pub fn session_id(&self) -> &str {
        match self {
            Session::Anonymous { session_id, .. } => session_id,
            Session::Identified { session_id, .. } => session_id,
        }
    }
}

impl TryFrom<Bytes> for Session {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let reader = bytes.as_ref();
        ciborium::de::from_reader(reader)
    }
}

impl TryInto<Bytes> for Session {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut writer = Vec::new();
        ciborium::ser::into_writer(&self, &mut writer)?;
        Ok(Bytes::from(writer))
    }
}

/// A session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OldSession {
    /// Identities verified in this session.
    pub identities: Vec<LedgerIdentity>,

    /// The session ID.
    pub session_id: String,

    /// The Ed25519 signing key for the server-side generated key.
    pub signing_key: Vec<u8>,

    /// The Ed25519 verifying key for the client-side generated key.
    pub verifying_key: Vec<u8>,
}

impl TryFrom<Bytes> for OldSession {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let reader = bytes.as_ref();
        ciborium::de::from_reader(reader)
    }
}

impl TryInto<Bytes> for OldSession {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut writer = Vec::new();
        ciborium::ser::into_writer(&self, &mut writer)?;
        Ok(Bytes::from(writer))
    }
}
