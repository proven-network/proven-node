use serde::{Serialize, Deserialize};
use super::{
    LedgerStateMixin, TransactionIntentStatus, TransactionStatus,
    TransactionStatusResponseKnownPayloadItem,
};
#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionStatusResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    ///If the intent was committed, this gives the state version when this intent was committed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_state_version: Option<i64>,
    /**The most relevant error message received, due to a rejection or commit as failure.
Please note that presence of an error message doesn't imply that the intent
will definitely reject or fail. This could represent a temporary error (such as out
of fees), or an error with a payload which doesn't end up being committed.*/
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    /**A more specific intent status. See the description field for further information.
Note that `CommitPendingOutcomeUnknown` can either result in `CommittedSuccess` or `CommittedFailure`.*/
    pub intent_status: TransactionIntentStatus,
    ///An additional description to clarify the intent status.
    pub intent_status_description: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub known_payloads: Vec<TransactionStatusResponseKnownPayloadItem>,
    ///The epoch number at which the transaction is guaranteed to get permanently rejected by the Network due to exceeded epoch range defined when submitting transaction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permanently_rejects_at_epoch: Option<i64>,
    /**A top-level intent status, left in for backwards compatibility.
It doesn't give much information. Rejected means PermanentRejection.*/
    pub status: TransactionStatus,
}
impl std::fmt::Display for TransactionStatusResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for TransactionStatusResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for TransactionStatusResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
