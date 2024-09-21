use serde::{Serialize, Deserialize};
use super::{EventsItem, TransactionStatus};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TransactionReceipt {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub costing_parameters: Option<serde_json::Value>,
    ///Error message (only present if status is `Failed` or `Rejected`)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    ///Events emitted by a transaction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub events: Option<Vec<EventsItem>>,
    ///This type is defined in the Core API as `FeeDestination`. See the Core API documentation for more details.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fee_destination: Option<serde_json::Value>,
    ///This type is defined in the Core API as `FeeSource`. See the Core API documentation for more details.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fee_source: Option<serde_json::Value>,
    ///This type is defined in the Core API as `FeeSummary`. See the Core API documentation for more details.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fee_summary: Option<serde_json::Value>,
    /**Information (number and active validator list) about new epoch if occured.
This type is defined in the Core API as `NextEpoch`. See the Core API documentation for more details.*/
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_epoch: Option<serde_json::Value>,
    /**The manifest line-by-line engine return data (only present if `status` is `CommittedSuccess`).
This type is defined in the Core API as `SborData`. See the Core API documentation for more details.*/
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,
    ///This type is defined in the Core API as `StateUpdates`. See the Core API documentation for more details.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_updates: Option<serde_json::Value>,
    /**A top-level intent status, left in for backwards compatibility.
It doesn't give much information. Rejected means PermanentRejection.*/
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<TransactionStatus>,
}
impl std::fmt::Display for TransactionReceipt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
