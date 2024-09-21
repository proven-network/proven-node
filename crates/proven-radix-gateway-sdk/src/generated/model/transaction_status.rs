use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TransactionStatus {
    Unknown,
    CommittedSuccess,
    CommittedFailure,
    Pending,
    Rejected,
}
