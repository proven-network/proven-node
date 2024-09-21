use serde::{Serialize, Deserialize};
use super::ResultSetCursorMixin;
///Non-fungible resource IDs collection.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct OptionalNonFungibleIdsCollection {
    #[serde(flatten)]
    pub result_set_cursor_mixin: ResultSetCursorMixin,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<String>,
}
impl std::fmt::Display for OptionalNonFungibleIdsCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for OptionalNonFungibleIdsCollection {
    type Target = ResultSetCursorMixin;
    fn deref(&self) -> &Self::Target {
        &self.result_set_cursor_mixin
    }
}
impl std::ops::DerefMut for OptionalNonFungibleIdsCollection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.result_set_cursor_mixin
    }
}
