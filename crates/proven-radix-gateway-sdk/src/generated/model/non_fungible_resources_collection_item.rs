use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct NonFungibleResourcesCollectionItem(pub serde_json::Value);
