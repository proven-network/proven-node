use super::{Log, ResourceChange};
use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TransactionPreviewResponse {
    ///Hex-encoded binary blob.
    pub encoded_receipt: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logs: Vec<Log>,
    /**An optional field which is only provided if the `request_radix_engine_toolkit_receipt`
    flag is set to true when requesting a transaction preview from the API.
    This receipt is primarily intended for use with the toolkit and may contain information
    that is already available in the receipt provided in the `receipt` field of this
    response.
    A typical client of this API is not expected to use this receipt. The primary clients
    this receipt is intended for is the Radix wallet or any client that needs to perform
    execution summaries on their transactions.*/
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub radix_engine_toolkit_receipt: Option<serde_json::Value>,
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
