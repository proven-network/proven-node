use serde::{Serialize, Deserialize};
use super::{LedgerStateMixin, NonFungibleIdsCollection};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateEntityNonFungibleIdsPageResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    ///Non-fungible resource IDs collection.
    #[serde(flatten)]
    pub non_fungible_ids_collection: NonFungibleIdsCollection,
    ///Bech32m-encoded human readable version of the address.
    pub address: String,
    ///Bech32m-encoded human readable version of the address.
    pub resource_address: String,
}
impl std::fmt::Display for StateEntityNonFungibleIdsPageResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateEntityNonFungibleIdsPageResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for StateEntityNonFungibleIdsPageResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
