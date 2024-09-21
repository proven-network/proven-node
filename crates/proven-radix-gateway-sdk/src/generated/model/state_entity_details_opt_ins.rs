use serde::{Serialize, Deserialize};
///Check detailed [OptIns](#section/Using-endpoints-with-opt-in-features) documentation for more details
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateEntityDetailsOptIns {
    ///if set to `true`, ancestor addresses - `parent_address`, `owner_address` and `global_address` for entities are returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ancestor_identities: Option<bool>,
    ///if set to `true`, `royalty_config` for component entities is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component_royalty_config: Option<bool>,
    ///if set to `true`, `royalty_vault_balance` for component entities is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component_royalty_vault_balance: Option<bool>,
    ///allows specifying explicitly metadata properties which should be returned in response.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explicit_metadata: Option<Vec<String>>,
    ///if set to `true`, first page of non fungible ids are returned for each non fungible resource, with `next_cursor` which can be later used at `/state/entity/page/non-fungible-vault/ids` endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub non_fungible_include_nfids: Option<bool>,
    ///if set to `true`, `royalty_vault_balance` for package entities is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub package_royalty_vault_balance: Option<bool>,
}
impl std::fmt::Display for StateEntityDetailsOptIns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
