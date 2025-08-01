use bytes::Bytes;
use serde::{Deserialize, Serialize};
use webauthn_rs::prelude::Passkey as WebauthnPasskey;

/// A passkey.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Passkey(WebauthnPasskey);

impl Passkey {
    /// Get a reference to the inner webauthn passkey.
    #[must_use]
    pub const fn inner(&self) -> &WebauthnPasskey {
        &self.0
    }

    /// Convert to the inner webauthn passkey.
    #[must_use]
    pub fn into_inner(self) -> WebauthnPasskey {
        self.0
    }
}

impl From<WebauthnPasskey> for Passkey {
    fn from(passkey: WebauthnPasskey) -> Self {
        Self(passkey)
    }
}

impl From<Passkey> for WebauthnPasskey {
    fn from(passkey: Passkey) -> Self {
        passkey.0
    }
}

impl TryFrom<Bytes> for Passkey {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        let reader = bytes.as_ref();
        ciborium::de::from_reader(reader)
    }
}

impl TryInto<Bytes> for Passkey {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut writer = Vec::new();
        ciborium::ser::into_writer(&self, &mut writer)?;
        Ok(Bytes::from(writer))
    }
}
