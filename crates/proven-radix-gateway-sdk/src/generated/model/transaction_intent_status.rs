use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TransactionIntentStatus {
    Unknown,
    CommittedSuccess,
    CommittedFailure,
    CommitPendingOutcomeUnknown,
    PermanentlyRejected,
    LikelyButNotCertainRejection,
    Pending,
}
