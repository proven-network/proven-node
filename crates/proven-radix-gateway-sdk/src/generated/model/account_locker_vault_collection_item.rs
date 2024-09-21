use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AccountLockerVaultCollectionItem(pub serde_json::Value);
