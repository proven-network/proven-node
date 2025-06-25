use crate::Application;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Responses returned by the application service after processing commands.
/// These indicate the result of command execution.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum ApplicationCommandResponse {
    /// Application was successfully archived.
    ApplicationArchived {
        /// The sequence number of the last event published for this command.
        last_event_seq: u64,
    },

    /// An application was successfully created.
    ApplicationCreated {
        /// The newly created application.
        application: Application,

        /// The sequence number of the last event published for this command.
        last_event_seq: u64,
    },

    /// Command failed with an error message.
    Error {
        /// Description of what went wrong.
        message: String,
    },

    /// Ownership transfer completed successfully.
    OwnershipTransferred {
        /// The sequence number of the last event published for this command.
        last_event_seq: u64,
    },
}

impl TryFrom<Bytes> for ApplicationCommandResponse {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, <Self as TryFrom<Bytes>>::Error> {
        let reader = bytes.as_ref();
        ciborium::de::from_reader(reader)
    }
}

impl TryInto<Bytes> for ApplicationCommandResponse {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, <Self as TryInto<Bytes>>::Error> {
        let mut writer = Vec::new();
        ciborium::ser::into_writer(&self, &mut writer)?;
        Ok(Bytes::from(writer))
    }
}
