use serde::{Serialize, Deserialize};
use super::{FungibleResourcesCollection, LedgerStateMixin};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateEntityFungiblesPageResponse {
    ///Fungible resources collection.
    #[serde(flatten)]
    pub fungible_resources_collection: FungibleResourcesCollection,
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    ///Bech32m-encoded human readable version of the address.
    pub address: String,
}
impl std::fmt::Display for StateEntityFungiblesPageResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateEntityFungiblesPageResponse {
    type Target = FungibleResourcesCollection;
    fn deref(&self) -> &Self::Target {
        &self.fungible_resources_collection
    }
}
impl std::ops::DerefMut for StateEntityFungiblesPageResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.fungible_resources_collection
    }
}
