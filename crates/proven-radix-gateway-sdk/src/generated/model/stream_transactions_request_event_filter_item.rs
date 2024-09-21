use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StreamTransactionsRequestEventFilterItem {
    ///Bech32m-encoded human readable version of the address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub emitter_address: Option<String>,
    pub event: String,
    ///Bech32m-encoded human readable version of the address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_address: Option<String>,
}
impl std::fmt::Display for StreamTransactionsRequestEventFilterItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
