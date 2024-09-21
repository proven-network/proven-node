use serde::{Serialize, Deserialize};
use super::{
    ManifestClass, TransactionBalanceChanges, TransactionReceipt, TransactionStatus,
};
#[derive(Debug, Serialize, Deserialize)]
pub struct CommittedTransactionInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affected_global_entities: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub balance_changes: Option<TransactionBalanceChanges>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirmed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub epoch: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    ///String-encoded decimal representing the amount of a related fungible resource.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fee_paid: Option<String>,
    ///Bech32m-encoded hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent_hash: Option<String>,
    /**A collection of zero or more manifest classes ordered from the most specific class to the least specific one.
This field will be present only for user transactions.*/
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_classes: Option<Vec<ManifestClass>>,
    /**A text-representation of a transaction manifest.
This field will be present only for user transactions and when explicitly opted-in using `manifest_instructions` flag.*/
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_instructions: Option<String>,
    /**The optional transaction message.
This type is defined in the Core API as `TransactionMessage`. See the Core API documentation for more details.*/
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<serde_json::Value>,
    ///Bech32m-encoded hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_hash: Option<String>,
    ///Hex-encoded binary blob.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_hex: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<TransactionReceipt>,
    pub round: i64,
    pub round_timestamp: String,
    pub state_version: i64,
    /**A top-level intent status, left in for backwards compatibility.
It doesn't give much information. Rejected means PermanentRejection.*/
    pub transaction_status: TransactionStatus,
}
impl std::fmt::Display for CommittedTransactionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
