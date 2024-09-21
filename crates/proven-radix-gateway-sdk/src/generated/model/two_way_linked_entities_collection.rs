use serde::{Serialize, Deserialize};
use super::{ResultSetCursorMixin, TwoWayLinkedEntitiesCollectionItem};
///A collection of two-way linked (resolved & verified) entities linked to the entity defining this collection.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TwoWayLinkedEntitiesCollection {
    #[serde(flatten)]
    pub result_set_cursor_mixin: ResultSetCursorMixin,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<TwoWayLinkedEntitiesCollectionItem>,
}
impl std::fmt::Display for TwoWayLinkedEntitiesCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for TwoWayLinkedEntitiesCollection {
    type Target = ResultSetCursorMixin;
    fn deref(&self) -> &Self::Target {
        &self.result_set_cursor_mixin
    }
}
impl std::ops::DerefMut for TwoWayLinkedEntitiesCollection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.result_set_cursor_mixin
    }
}
