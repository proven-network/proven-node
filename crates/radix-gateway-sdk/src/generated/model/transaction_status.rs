use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TransactionStatus {
    Unknown,
    CommittedSuccess,
    CommittedFailure,
    Pending,
    Rejected,
}
