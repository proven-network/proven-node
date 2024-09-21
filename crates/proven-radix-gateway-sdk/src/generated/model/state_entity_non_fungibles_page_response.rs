use serde::{Serialize, Deserialize};
use super::{LedgerStateMixin, NonFungibleResourcesCollection};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateEntityNonFungiblesPageResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    ///Non-fungible resources collection.
    #[serde(flatten)]
    pub non_fungible_resources_collection: NonFungibleResourcesCollection,
    ///Bech32m-encoded human readable version of the address.
    pub address: String,
}
impl std::fmt::Display for StateEntityNonFungiblesPageResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateEntityNonFungiblesPageResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for StateEntityNonFungiblesPageResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
