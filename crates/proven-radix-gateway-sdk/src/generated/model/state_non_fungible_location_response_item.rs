use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateNonFungibleLocationResponseItem {
    pub is_burned: bool,
    ///The most recent state version underlying object was modified at.
    pub last_updated_at_state_version: i64,
    ///String-encoded non-fungible ID.
    pub non_fungible_id: String,
    ///Bech32m-encoded human readable version of the address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owning_vault_address: Option<String>,
    ///Bech32m-encoded human readable version of the address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owning_vault_global_ancestor_address: Option<String>,
    ///Bech32m-encoded human readable version of the address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owning_vault_parent_ancestor_address: Option<String>,
}
impl std::fmt::Display for StateNonFungibleLocationResponseItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
