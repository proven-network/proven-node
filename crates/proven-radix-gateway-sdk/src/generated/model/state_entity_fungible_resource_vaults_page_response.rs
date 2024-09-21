use serde::{Serialize, Deserialize};
use super::{FungibleResourcesCollectionItemVaultAggregatedVault, LedgerStateMixin};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateEntityFungibleResourceVaultsPageResponse {
    #[serde(flatten)]
    pub fungible_resources_collection_item_vault_aggregated_vault: FungibleResourcesCollectionItemVaultAggregatedVault,
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    ///Bech32m-encoded human readable version of the address.
    pub address: String,
    ///Bech32m-encoded human readable version of the address.
    pub resource_address: String,
}
impl std::fmt::Display for StateEntityFungibleResourceVaultsPageResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateEntityFungibleResourceVaultsPageResponse {
    type Target = FungibleResourcesCollectionItemVaultAggregatedVault;
    fn deref(&self) -> &Self::Target {
        &self.fungible_resources_collection_item_vault_aggregated_vault
    }
}
impl std::ops::DerefMut for StateEntityFungibleResourceVaultsPageResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.fungible_resources_collection_item_vault_aggregated_vault
    }
}
