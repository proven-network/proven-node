use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TwoWayLinkedEntitiesCollectionItem {
    ///Bech32m-encoded human readable version of the address.
    pub entity_address: String,
}
impl std::fmt::Display for TwoWayLinkedEntitiesCollectionItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
