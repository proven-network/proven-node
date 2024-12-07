use proven_radix_gateway_sdk::types::EventsItem;

use bytes::Bytes;

/// An event emitted by a transaction on the Radix ledger.
#[derive(Clone, Debug)]
pub struct Event(pub(crate) EventsItem);

impl TryFrom<Bytes> for Event {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        Ok(Self(ciborium::de::from_reader(bytes.as_ref())?))
    }
}

impl TryInto<Bytes> for Event {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&self.0, &mut bytes)?;
        Ok(Bytes::from(bytes))
    }
}
