use serde::{Serialize, Deserialize};
use super::ProgrammaticScryptoSborValue;
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct EventsItem {
    pub data: ProgrammaticScryptoSborValue,
    ///This type is defined in the Core API as `EventEmitterIdentifier`. See the Core API documentation for more details.
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub emitter: serde_json::Value,
    pub name: String,
}
impl std::fmt::Display for EventsItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
