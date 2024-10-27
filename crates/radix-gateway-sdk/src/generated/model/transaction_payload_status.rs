use serde::{Deserialize, Serialize};
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
