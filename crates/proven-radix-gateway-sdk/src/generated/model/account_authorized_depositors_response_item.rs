use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AccountAuthorizedDepositorsResponseItem(pub serde_json::Value);
