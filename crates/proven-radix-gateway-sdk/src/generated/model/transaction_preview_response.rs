use serde::{Serialize, Deserialize};
use super::{Log, ResourceChange};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TransactionPreviewResponse {
    ///Hex-encoded binary blob.
    pub encoded_receipt: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logs: Vec<Log>,
    ///This type is defined in the Core API as `TransactionReceipt`. See the Core API documentation for more details.
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub receipt: serde_json::Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_changees: Vec<ResourceChange>,
}
impl std::fmt::Display for TransactionPreviewResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
