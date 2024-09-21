use serde::{Serialize, Deserialize};
use super::{LedgerStateMixin, NonFungibleIdsCollection};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateNonFungibleIdsResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    ///Non-fungible resource IDs collection.
    pub non_fungible_ids: NonFungibleIdsCollection,
    ///Bech32m-encoded human readable version of the address.
    pub resource_address: String,
}
impl std::fmt::Display for StateNonFungibleIdsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateNonFungibleIdsResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for StateNonFungibleIdsResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
