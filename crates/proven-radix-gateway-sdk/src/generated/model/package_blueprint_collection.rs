use serde::{Serialize, Deserialize};
use super::{PackageBlueprintCollectionItem, ResultSetCursorMixin};
///Package blueprint collection.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct PackageBlueprintCollection {
    #[serde(flatten)]
    pub result_set_cursor_mixin: ResultSetCursorMixin,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<PackageBlueprintCollectionItem>,
}
impl std::fmt::Display for PackageBlueprintCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for PackageBlueprintCollection {
    type Target = ResultSetCursorMixin;
    fn deref(&self) -> &Self::Target {
        &self.result_set_cursor_mixin
    }
}
impl std::ops::DerefMut for PackageBlueprintCollection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.result_set_cursor_mixin
    }
}
