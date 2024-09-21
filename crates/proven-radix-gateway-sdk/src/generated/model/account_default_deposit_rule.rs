use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum AccountDefaultDepositRule {
    Accept,
    Reject,
    AllowExisting,
}
