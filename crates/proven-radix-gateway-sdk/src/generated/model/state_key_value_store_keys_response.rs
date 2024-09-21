use serde::{Serialize, Deserialize};
use super::{LedgerStateMixin, StateKeyValueStoreKeysCollection};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateKeyValueStoreKeysResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    ///Key value store items collection.
    #[serde(flatten)]
    pub state_key_value_store_keys_collection: StateKeyValueStoreKeysCollection,
    ///Bech32m-encoded human readable version of the address.
    pub key_value_store_address: String,
}
impl std::fmt::Display for StateKeyValueStoreKeysResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateKeyValueStoreKeysResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for StateKeyValueStoreKeysResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
