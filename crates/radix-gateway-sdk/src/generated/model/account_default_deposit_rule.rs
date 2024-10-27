use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum AccountDefaultDepositRule {
    Accept,
    Reject,
    AllowExisting,
}
