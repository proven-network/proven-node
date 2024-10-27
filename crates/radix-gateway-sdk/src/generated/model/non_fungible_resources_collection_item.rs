use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct NonFungibleResourcesCollectionItem(pub serde_json::Value);
