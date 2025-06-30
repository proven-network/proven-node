use bytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A session - either for application-specific operations or management operations
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Session {
    /// Session scoped to a specific application
    Application(ApplicationSession),
    /// Session for management operations (not tied to a specific application)
    Management(ManagementSession),
}

/// Session for application-specific operations
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ApplicationSession {
    /// Anonymous session within an application context
    Anonymous {
        /// The application ID this session is scoped to
        application_id: Uuid,
        /// The origin of the session
        origin: String,
        /// The session ID
        session_id: Uuid,
        /// The Ed25519 signing key for the server-side generated key
        signing_key: SigningKey,
        /// The Ed25519 verifying key for the client-side generated key
        verifying_key: VerifyingKey,
    },
    /// Identified session within an application context
    Identified {
        /// The application ID this session is scoped to
        application_id: Uuid,
        /// The Proven identity
        identity_id: Uuid,
        /// The origin of the session
        origin: String,
        /// The session ID
        session_id: Uuid,
        /// The Ed25519 signing key for the server-side generated key
        signing_key: SigningKey,
        /// The Ed25519 verifying key for the client-side generated key
        verifying_key: VerifyingKey,
    },
}

/// Session for management operations
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ManagementSession {
    /// Anonymous management session
    Anonymous {
        /// The origin of the session
        origin: String,
        /// The session ID
        session_id: Uuid,
        /// The Ed25519 signing key for the server-side generated key
        signing_key: SigningKey,
        /// The Ed25519 verifying key for the client-side generated key
        verifying_key: VerifyingKey,
    },
    /// Identified management session
    Identified {
        /// The Proven identity
        identity_id: Uuid,
        /// The origin of the session
        origin: String,
        /// The session ID
        session_id: Uuid,
        /// The Ed25519 signing key for the server-side generated key
        signing_key: SigningKey,
        /// The Ed25519 verifying key for the client-side generated key
        verifying_key: VerifyingKey,
    },
}

impl Session {
    /// The origin of the session.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn origin(&self) -> &str {
        match self {
            Self::Application(app_session) => app_session.origin(),
            Self::Management(mgmt_session) => mgmt_session.origin(),
        }
    }

    /// The session ID.
    #[must_use]
    pub const fn session_id(&self) -> &Uuid {
        match self {
            Self::Application(app_session) => app_session.session_id(),
            Self::Management(mgmt_session) => mgmt_session.session_id(),
        }
    }

    /// The Ed25519 signing key for the server-side generated key.
    #[must_use]
    pub const fn signing_key(&self) -> &SigningKey {
        match self {
            Self::Application(app_session) => app_session.signing_key(),
            Self::Management(mgmt_session) => mgmt_session.signing_key(),
        }
    }

    /// The Ed25519 verifying key for the client-side generated key.
    #[must_use]
    pub const fn verifying_key(&self) -> &VerifyingKey {
        match self {
            Self::Application(app_session) => app_session.verifying_key(),
            Self::Management(mgmt_session) => mgmt_session.verifying_key(),
        }
    }

    /// Returns true if this is an application session
    #[must_use]
    pub const fn is_application_session(&self) -> bool {
        matches!(self, Self::Application(_))
    }

    /// Returns true if this is a management session
    #[must_use]
    pub const fn is_management_session(&self) -> bool {
        matches!(self, Self::Management(_))
    }

    /// Returns the application ID if this is an application session
    #[must_use]
    pub const fn application_id(&self) -> Option<&Uuid> {
        match self {
            Self::Application(app_session) => Some(app_session.application_id()),
            Self::Management(_) => None,
        }
    }

    /// Returns the identity ID if this is an identified session
    #[must_use]
    pub const fn identity_id(&self) -> Option<&Uuid> {
        match self {
            Self::Application(app_session) => app_session.identity_id(),
            Self::Management(mgmt_session) => mgmt_session.identity_id(),
        }
    }
}

impl ApplicationSession {
    /// The application ID this session is scoped to
    #[must_use]
    pub const fn application_id(&self) -> &Uuid {
        match self {
            Self::Anonymous { application_id, .. } | Self::Identified { application_id, .. } => {
                application_id
            }
        }
    }

    /// The origin of the session
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn origin(&self) -> &str {
        match self {
            Self::Anonymous { origin, .. } | Self::Identified { origin, .. } => origin,
        }
    }

    /// The session ID
    #[must_use]
    pub const fn session_id(&self) -> &Uuid {
        match self {
            Self::Anonymous { session_id, .. } | Self::Identified { session_id, .. } => session_id,
        }
    }

    /// The Ed25519 signing key for the server-side generated key
    #[must_use]
    pub const fn signing_key(&self) -> &SigningKey {
        match self {
            Self::Anonymous { signing_key, .. } | Self::Identified { signing_key, .. } => {
                signing_key
            }
        }
    }

    /// The Ed25519 verifying key for the client-side generated key
    #[must_use]
    pub const fn verifying_key(&self) -> &VerifyingKey {
        match self {
            Self::Anonymous { verifying_key, .. } | Self::Identified { verifying_key, .. } => {
                verifying_key
            }
        }
    }

    /// Returns the identity ID if this is an identified session
    #[must_use]
    pub const fn identity_id(&self) -> Option<&Uuid> {
        match self {
            Self::Anonymous { .. } => None,
            Self::Identified { identity_id, .. } => Some(identity_id),
        }
    }
}

impl ManagementSession {
    /// The origin of the session
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn origin(&self) -> &str {
        match self {
            Self::Anonymous { origin, .. } | Self::Identified { origin, .. } => origin,
        }
    }

    /// The session ID
    #[must_use]
    pub const fn session_id(&self) -> &Uuid {
        match self {
            Self::Anonymous { session_id, .. } | Self::Identified { session_id, .. } => session_id,
        }
    }

    /// The Ed25519 signing key for the server-side generated key
    #[must_use]
    pub const fn signing_key(&self) -> &SigningKey {
        match self {
            Self::Anonymous { signing_key, .. } | Self::Identified { signing_key, .. } => {
                signing_key
            }
        }
    }

    /// The Ed25519 verifying key for the client-side generated key
    #[must_use]
    pub const fn verifying_key(&self) -> &VerifyingKey {
        match self {
            Self::Anonymous { verifying_key, .. } | Self::Identified { verifying_key, .. } => {
                verifying_key
            }
        }
    }

    /// Returns the identity ID if this is an identified session
    #[must_use]
    pub const fn identity_id(&self) -> Option<&Uuid> {
        match self {
            Self::Anonymous { .. } => None,
            Self::Identified { identity_id, .. } => Some(identity_id),
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
