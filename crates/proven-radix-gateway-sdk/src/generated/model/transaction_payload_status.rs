use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TransactionPayloadStatus {
    Unknown,
    CommittedSuccess,
    CommittedFailure,
    CommitPendingOutcomeUnknown,
    PermanentlyRejected,
    TemporarilyRejected,
    Pending,
}
