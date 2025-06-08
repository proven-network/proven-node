use crate::identity::Identity;
use crate::ledger_identity::LedgerIdentity;

use bytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Session {
    /// An anonymous session - yet to be identified via a ledger ID handshake.
    Anonymous {
        /// The origin of the session.
        origin: String,

        /// The session ID.
        session_id: Uuid,

        /// The Ed25519 signing key for the server-side generated key.
        signing_key: SigningKey,

        /// The Ed25519 verifying key for the client-side generated key.
        verifying_key: VerifyingKey,
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
        session_id: Uuid,

        /// The Ed25519 signing key for the server-side generated key.
        signing_key: SigningKey,

        /// The Ed25519 verifying key for the client-side generated key.
        verifying_key: VerifyingKey,
    },
}

impl Session {
    /// The origin of the session.
    #[must_use]
    pub fn origin(&self) -> &str {
        match self {
            Self::Anonymous { origin, .. } | Self::Identified { origin, .. } => origin,
        }
    }

    /// The session ID.
    #[must_use]
    pub fn session_id(&self) -> &Uuid {
        match self {
            Self::Anonymous { session_id, .. } | Self::Identified { session_id, .. } => session_id,
        }
    }

    /// The Ed25519 signing key for the server-side generated key.
    #[must_use]
    pub const fn signing_key(&self) -> &SigningKey {
        match self {
            Self::Anonymous { signing_key, .. } | Self::Identified { signing_key, .. } => {
                signing_key
            }
        }
    }

    /// The Ed25519 verifying key for the client-side generated key.
    #[must_use]
    pub const fn verifying_key(&self) -> &VerifyingKey {
        match self {
            Self::Anonymous { verifying_key, .. } | Self::Identified { verifying_key, .. } => {
                verifying_key
            }
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
