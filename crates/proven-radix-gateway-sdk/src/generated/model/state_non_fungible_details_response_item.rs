use serde::{Serialize, Deserialize};
use super::ScryptoSborValue;
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateNonFungibleDetailsResponseItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<ScryptoSborValue>,
    pub is_burned: bool,
    ///The most recent state version underlying object was modified at.
    pub last_updated_at_state_version: i64,
    ///String-encoded non-fungible ID.
    pub non_fungible_id: String,
}
impl std::fmt::Display for StateNonFungibleDetailsResponseItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
