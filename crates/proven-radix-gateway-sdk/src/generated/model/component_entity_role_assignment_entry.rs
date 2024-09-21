use serde::{Serialize, Deserialize};
use super::RoleKey;
#[derive(Debug, Serialize, Deserialize)]
pub struct ComponentEntityRoleAssignmentEntry {
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub assignment: serde_json::Value,
    pub role_key: RoleKey,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updater_roles: Option<Vec<RoleKey>>,
}
impl std::fmt::Display for ComponentEntityRoleAssignmentEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
