use serde::{Serialize, Deserialize};
use super::OptionalNonFungibleIdsCollection;
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct NonFungibleResourcesCollectionItemVaultAggregatedVaultItem {
    ///Non-fungible resource IDs collection.
    #[serde(flatten)]
    pub optional_non_fungible_ids_collection: OptionalNonFungibleIdsCollection,
    ///The most recent state version underlying object was modified at.
    pub last_updated_at_state_version: i64,
    pub total_count: i64,
    ///Bech32m-encoded human readable version of the address.
    pub vault_address: String,
}
impl std::fmt::Display for NonFungibleResourcesCollectionItemVaultAggregatedVaultItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for NonFungibleResourcesCollectionItemVaultAggregatedVaultItem {
    type Target = OptionalNonFungibleIdsCollection;
    fn deref(&self) -> &Self::Target {
        &self.optional_non_fungible_ids_collection
    }
}
impl std::ops::DerefMut for NonFungibleResourcesCollectionItemVaultAggregatedVaultItem {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.optional_non_fungible_ids_collection
    }
}
