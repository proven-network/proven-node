use serde::{Serialize, Deserialize};
use super::{EntitySchemaCollectionItem, ResultSetCursorMixin};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct EntitySchemaCollection {
    #[serde(flatten)]
    pub result_set_cursor_mixin: ResultSetCursorMixin,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<EntitySchemaCollectionItem>,
}
impl std::fmt::Display for EntitySchemaCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for EntitySchemaCollection {
    type Target = ResultSetCursorMixin;
    fn deref(&self) -> &Self::Target {
        &self.result_set_cursor_mixin
    }
}
impl std::ops::DerefMut for EntitySchemaCollection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.result_set_cursor_mixin
    }
}
