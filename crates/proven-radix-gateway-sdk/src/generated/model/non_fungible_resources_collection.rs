use serde::{Serialize, Deserialize};
use super::{NonFungibleResourcesCollectionItem, ResultSetCursorMixin};
///Non-fungible resources collection.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct NonFungibleResourcesCollection {
    #[serde(flatten)]
    pub result_set_cursor_mixin: ResultSetCursorMixin,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<NonFungibleResourcesCollectionItem>,
}
impl std::fmt::Display for NonFungibleResourcesCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for NonFungibleResourcesCollection {
    type Target = ResultSetCursorMixin;
    fn deref(&self) -> &Self::Target {
        &self.result_set_cursor_mixin
    }
}
impl std::ops::DerefMut for NonFungibleResourcesCollection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.result_set_cursor_mixin
    }
}
