use serde::{Serialize, Deserialize};
///This type is defined in the Core API as `InstructionResourceChanges`. See the Core API documentation for more details.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ResourceChange {}
impl std::fmt::Display for ResourceChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
