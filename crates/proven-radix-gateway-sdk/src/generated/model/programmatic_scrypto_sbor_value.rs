use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ProgrammaticScryptoSborValue(pub serde_json::Value);
