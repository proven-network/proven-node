use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TransactionAccountDepositPreValidationAuthorizedDepositorBadge(pub serde_json::Value);
