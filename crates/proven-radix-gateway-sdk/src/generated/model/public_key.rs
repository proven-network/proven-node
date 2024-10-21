use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct PublicKey(pub serde_json::Value);
