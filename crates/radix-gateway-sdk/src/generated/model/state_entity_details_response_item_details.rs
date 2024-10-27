use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateEntityDetailsResponseItemDetails(pub serde_json::Value);
