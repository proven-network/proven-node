use serde::{Serialize, Deserialize};
use super::{
    TransactionPayloadGatewayHandlingStatus, TransactionPayloadStatus, TransactionStatus,
};
#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionStatusResponseKnownPayloadItem {
    /**The initial error message received for a rejection or failure during transaction execution.
This will typically be the useful error message, explaining the root cause of the issue.
Please note that presence of an error message doesn't imply that this payload
will definitely reject or fail. This could represent an error during a temporary
rejection (such as out of fees) which then gets resolved (e.g. by depositing money
to pay the fee), allowing the transaction to be committed.*/
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    ///A status concerning the Gateway's handling status of this pending transaction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handling_status: Option<TransactionPayloadGatewayHandlingStatus>,
    ///Additional reason for why the Gateway has its current handling status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handling_status_reason: Option<String>,
    /**The latest error message received for a rejection or failure during transaction execution,
this is only returned if it is different from the initial error message.
This is more current than the initial error message, but may be less useful, as it could
be a message regarding the expiry of the transaction at the end of its epoch validity window.
Please note that presence of an error message doesn't imply that this payload
will definitely reject or fail. This could represent an error during a temporary
rejection (such as out of fees) which then gets resolved (e.g. by depositing money
to pay the fee), allowing the transaction to be committed.*/
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_error_message: Option<String>,
    ///Bech32m-encoded hash.
    pub payload_hash: String,
    ///A more specific payload status. See the description field for further information.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_status: Option<TransactionPayloadStatus>,
    ///An additional description to clarify the payload status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_status_description: Option<String>,
    /**A top-level intent status, left in for backwards compatibility.
It doesn't give much information. Rejected means PermanentRejection.*/
    pub status: TransactionStatus,
    /**The most recent error message received when submitting this transaction to the network.
Please note that the presence of an error message doesn't imply that this transaction
payload will definitely reject or fail. This could be a transient error.*/
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_error: Option<String>,
}
impl std::fmt::Display for TransactionStatusResponseKnownPayloadItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
