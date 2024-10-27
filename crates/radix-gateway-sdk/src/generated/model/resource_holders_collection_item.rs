use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ResourceHoldersCollectionItem(pub serde_json::Value);
