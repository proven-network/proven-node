use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateEntityDetailsResponseItemAncestorIdentities {
    ///Bech32m-encoded human readable version of the address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_address: Option<String>,
    ///Bech32m-encoded human readable version of the address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_address: Option<String>,
    ///Bech32m-encoded human readable version of the address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_address: Option<String>,
}
impl std::fmt::Display for StateEntityDetailsResponseItemAncestorIdentities {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
