use serde::{Serialize, Deserialize};
use super::{LedgerStateMixin, StateNonFungibleLocationResponseItem};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateNonFungibleLocationResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub non_fungible_ids: Vec<StateNonFungibleLocationResponseItem>,
    ///Bech32m-encoded human readable version of the address.
    pub resource_address: String,
}
impl std::fmt::Display for StateNonFungibleLocationResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateNonFungibleLocationResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for StateNonFungibleLocationResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
