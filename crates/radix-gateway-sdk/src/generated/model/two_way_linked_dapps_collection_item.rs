use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TwoWayLinkedDappsCollectionItem {
    ///Bech32m-encoded human readable version of the address.
    pub dapp_address: String,
}
impl std::fmt::Display for TwoWayLinkedDappsCollectionItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
