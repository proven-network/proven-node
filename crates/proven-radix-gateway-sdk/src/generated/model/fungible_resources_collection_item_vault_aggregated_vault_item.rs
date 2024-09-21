use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct FungibleResourcesCollectionItemVaultAggregatedVaultItem {
    ///String-encoded decimal representing the amount of a related fungible resource.
    pub amount: String,
    ///The most recent state version underlying object was modified at.
    pub last_updated_at_state_version: i64,
    ///Bech32m-encoded human readable version of the address.
    pub vault_address: String,
}
impl std::fmt::Display for FungibleResourcesCollectionItemVaultAggregatedVaultItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
