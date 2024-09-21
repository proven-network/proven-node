use serde::{Serialize, Deserialize};
///Check detailed [OptIns](#section/Using-endpoints-with-opt-in-features) documentation for more details
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateEntityNonFungibleResourceVaultsPageOptIns {
    ///if set to `true`, first page of non fungible ids are returned for each non fungible resource, with cursor which can be later used at `/state/entity/page/non-fungible-vault/ids` endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub non_fungible_include_nfids: Option<bool>,
}
impl std::fmt::Display for StateEntityNonFungibleResourceVaultsPageOptIns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
