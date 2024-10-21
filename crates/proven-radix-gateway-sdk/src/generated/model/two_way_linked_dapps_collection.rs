use super::{ResultSetCursorMixin, TwoWayLinkedDappsCollectionItem};
use serde::{Deserialize, Serialize};
///A collection of two-way linked (resolved & verified) dApps linked to the entity defining this collection.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TwoWayLinkedDappsCollection {
    #[serde(flatten)]
    pub result_set_cursor_mixin: ResultSetCursorMixin,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<TwoWayLinkedDappsCollectionItem>,
}
impl std::fmt::Display for TwoWayLinkedDappsCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for TwoWayLinkedDappsCollection {
    type Target = ResultSetCursorMixin;
    fn deref(&self) -> &Self::Target {
        &self.result_set_cursor_mixin
    }
}
impl std::ops::DerefMut for TwoWayLinkedDappsCollection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.result_set_cursor_mixin
    }
}
