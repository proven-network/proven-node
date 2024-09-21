use serde::{Serialize, Deserialize};
use super::EntityMetadataItemValue;
///Entity metadata key-value pair.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct EntityMetadataItem {
    pub is_locked: bool,
    ///Entity metadata key.
    pub key: String,
    ///The most recent state version underlying object was modified at.
    pub last_updated_at_state_version: i64,
    pub value: EntityMetadataItemValue,
}
impl std::fmt::Display for EntityMetadataItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
