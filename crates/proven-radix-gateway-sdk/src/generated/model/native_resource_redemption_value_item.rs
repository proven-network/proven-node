use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct NativeResourceRedemptionValueItem {
    ///String-encoded decimal representing the amount of a related fungible resource.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amount: Option<String>,
    ///Bech32m-encoded human readable version of the address.
    pub resource_address: String,
}
impl std::fmt::Display for NativeResourceRedemptionValueItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
