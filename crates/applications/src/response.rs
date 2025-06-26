use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Responses returned by the application service after processing commands.
/// These indicate the result of command execution.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Response {
    /// An allowed origin was successfully added to an application.
    AllowedOriginAdded {
        /// The sequence number of the last event published for this command.
        last_event_seq: u64,
    },

    /// An allowed origin was successfully removed from an application.
    AllowedOriginRemoved {
        /// The sequence number of the last event published for this command.
        last_event_seq: u64,
    },

    /// Application was successfully archived.
    Archived {
        /// The sequence number of the last event published for this command.
        last_event_seq: u64,
    },

    /// An application was successfully created.
    Created {
        /// The ID of the newly created application.
        application_id: Uuid,

        /// The sequence number of the last event published for this command.
        last_event_seq: u64,
    },

    /// Command failed with an error message.
    Error {
        /// Description of what went wrong.
        message: String,
    },

    /// An HTTP domain was successfully linked to an application.
    HttpDomainLinked {
        /// The sequence number of the last event published for this command.
        last_event_seq: u64,
    },

    /// An HTTP domain was successfully unlinked from an application.
    HttpDomainUnlinked {
        /// The sequence number of the last event published for this command.
        last_event_seq: u64,
    },

    /// Ownership transfer completed successfully.
    OwnershipTransferred {
        /// The sequence number of the last event published for this command.
        last_event_seq: u64,
    },
}

impl TryFrom<Bytes> for Response {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, <Self as TryFrom<Bytes>>::Error> {
        let reader = bytes.as_ref();
        ciborium::de::from_reader(reader)
    }
}

impl TryInto<Bytes> for Response {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, <Self as TryInto<Bytes>>::Error> {
        let mut writer = Vec::new();
        ciborium::ser::into_writer(&self, &mut writer)?;
        Ok(Bytes::from(writer))
    }
}
