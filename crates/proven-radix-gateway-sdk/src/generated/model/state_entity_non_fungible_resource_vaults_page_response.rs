use serde::{Serialize, Deserialize};
use super::{LedgerStateMixin, NonFungibleResourcesCollectionItemVaultAggregatedVault};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateEntityNonFungibleResourceVaultsPageResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    #[serde(flatten)]
    pub non_fungible_resources_collection_item_vault_aggregated_vault: NonFungibleResourcesCollectionItemVaultAggregatedVault,
    ///Bech32m-encoded human readable version of the address.
    pub address: String,
    ///Bech32m-encoded human readable version of the address.
    pub resource_address: String,
}
impl std::fmt::Display for StateEntityNonFungibleResourceVaultsPageResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateEntityNonFungibleResourceVaultsPageResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for StateEntityNonFungibleResourceVaultsPageResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
