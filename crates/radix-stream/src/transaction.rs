use crate::Event;

use bytes::Bytes;
use proven_radix_gateway_sdk::types::CommittedTransactionInfo;

/// A transaction on the Radix ledger.
#[derive(Clone, Debug)]
pub struct Transaction(pub(crate) CommittedTransactionInfo);

impl Transaction {
    /// Returns the events emitted by the transaction.
    #[must_use]
    pub fn events(&self) -> Vec<Event> {
        self.0.receipt.as_ref().map_or_else(Vec::new, |receipt| {
            receipt
                .events
                .iter()
                .map(|event| Event(event.clone()))
                .collect()
        })
    }

    /// Returns the state version of the transaction.
    #[must_use]
    pub fn state_version(&self) -> u64 {
        self.0.state_version.try_into().unwrap_or(0)
    }
}

impl From<CommittedTransactionInfo> for Transaction {
    fn from(info: CommittedTransactionInfo) -> Self {
        Self(info)
    }
}

impl TryFrom<Bytes> for Transaction {
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        Ok(Self(ciborium::de::from_reader(bytes.as_ref())?))
    }
}

impl TryInto<Bytes> for Transaction {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&self.0, &mut bytes)?;
        Ok(Bytes::from(bytes))
    }
}
