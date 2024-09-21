use serde::{Serialize, Deserialize};
use super::ObjectModuleId;
#[derive(Debug, Serialize, Deserialize)]
pub struct RoleKey {
    pub module: ObjectModuleId,
    pub name: String,
}
impl std::fmt::Display for RoleKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
