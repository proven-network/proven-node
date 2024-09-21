use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct MetadataTypedValue(pub serde_json::Value);
