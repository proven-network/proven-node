use serde::{Serialize, Deserialize};
use super::ProgrammaticScryptoSborValue;
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ScryptoSborValue {
    pub programmatic_json: ProgrammaticScryptoSborValue,
    ///Hex-encoded binary blob.
    pub raw_hex: String,
}
impl std::fmt::Display for ScryptoSborValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
