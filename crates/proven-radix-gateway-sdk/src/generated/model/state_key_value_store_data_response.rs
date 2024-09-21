use serde::{Serialize, Deserialize};
use super::{LedgerStateMixin, StateKeyValueStoreDataResponseItem};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateKeyValueStoreDataResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<StateKeyValueStoreDataResponseItem>,
    ///Bech32m-encoded human readable version of the address.
    pub key_value_store_address: String,
}
impl std::fmt::Display for StateKeyValueStoreDataResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateKeyValueStoreDataResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for StateKeyValueStoreDataResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
