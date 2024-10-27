use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct FungibleResourcesCollectionItem(pub serde_json::Value);
