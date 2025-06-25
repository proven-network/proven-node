use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Commands that can be sent to the application service to modify application state.
/// These commands are processed through the command stream and may succeed or fail.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ApplicationCommand {
    /// Archive an application, removing it from active use.
    ArchiveApplication {
        /// The unique identifier of the application to archive.
        application_id: Uuid,
    },

    /// Create a new application with the specified owner.
    CreateApplication {
        /// The identity ID of the user who will own the application.
        owner_identity_id: Uuid,
    },

    /// Transfer ownership of an application to a new owner.
    TransferOwnership {
        /// The unique identifier of the application to transfer.
        application_id: Uuid,

        /// The identity ID of the new owner.
        new_owner_id: Uuid,
    },
}

impl TryFrom<Bytes> for ApplicationCommand {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let reader = bytes.as_ref();
        ciborium::de::from_reader(reader)
    }
}

impl TryInto<Bytes> for ApplicationCommand {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut writer = Vec::new();
        ciborium::ser::into_writer(&self, &mut writer)?;
        Ok(Bytes::from(writer))
    }
}
