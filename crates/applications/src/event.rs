use bytes::Bytes;
use proven_util::{Domain, Origin};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Events that represent state changes in the application lifecycle.
/// These events are published to the event stream and consumed by various subsystems.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Event {
    /// An allowed origin was added to an application.
    AllowedOriginAdded {
        /// The unique identifier of the application.
        application_id: Uuid,

        /// The origin that was added.
        origin: Origin,

        /// The timestamp when the origin was added.
        added_at: chrono::DateTime<chrono::Utc>,
    },

    /// An allowed origin was removed from an application.
    AllowedOriginRemoved {
        /// The unique identifier of the application.
        application_id: Uuid,

        /// The origin that was removed.
        origin: Origin,

        /// The timestamp when the origin was removed.
        removed_at: chrono::DateTime<chrono::Utc>,
    },

    /// An application was archived and removed from active use.
    Archived {
        /// The unique identifier of the archived application.
        application_id: Uuid,

        /// The timestamp when the application was archived.
        archived_at: chrono::DateTime<chrono::Utc>,
    },

    /// An application was successfully created.
    Created {
        /// The unique identifier of the created application.
        application_id: Uuid,

        /// The timestamp when the application was created.
        created_at: chrono::DateTime<chrono::Utc>,

        /// The identity ID of the application owner.
        owner_identity_id: Uuid,
    },

    /// An HTTP domain was linked to an application.
    HttpDomainLinked {
        /// The unique identifier of the application.
        application_id: Uuid,

        /// The HTTP domain that was linked.
        http_domain: Domain,

        /// The timestamp when the HTTP domain was linked.
        linked_at: chrono::DateTime<chrono::Utc>,
    },

    /// An HTTP domain was unlinked from an application.
    HttpDomainUnlinked {
        /// The unique identifier of the application.
        application_id: Uuid,

        /// The HTTP domain that was unlinked.
        http_domain: Domain,

        /// The timestamp when the HTTP domain was unlinked.
        unlinked_at: chrono::DateTime<chrono::Utc>,
    },

    /// Ownership of an application was transferred to a new owner.
    OwnershipTransferred {
        /// The unique identifier of the application.
        application_id: Uuid,

        /// The identity ID of the new owner.
        new_owner_id: Uuid,

        /// The identity ID of the previous owner.
        old_owner_id: Uuid,

        /// The timestamp when the ownership was transferred.
        transferred_at: chrono::DateTime<chrono::Utc>,
    },
}

impl TryFrom<Bytes> for Event {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let reader = bytes.as_ref();
        ciborium::de::from_reader(reader)
    }
}

impl TryInto<Bytes> for Event {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut writer = Vec::new();
        ciborium::ser::into_writer(&self, &mut writer)?;
        Ok(Bytes::from(writer))
    }
}
