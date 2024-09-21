use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ResourceHoldersCollectionItem(pub serde_json::Value);
