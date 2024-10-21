use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AccountLockerVaultCollectionItem(pub serde_json::Value);
