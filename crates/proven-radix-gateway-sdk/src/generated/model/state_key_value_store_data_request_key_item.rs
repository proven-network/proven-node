use serde::{Serialize, Deserialize};
use super::ProgrammaticScryptoSborValue;
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateKeyValueStoreDataRequestKeyItem {
    ///Hex-encoded binary blob.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_hex: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_json: Option<ProgrammaticScryptoSborValue>,
}
impl std::fmt::Display for StateKeyValueStoreDataRequestKeyItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
