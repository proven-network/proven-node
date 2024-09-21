use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ResultSetCursorMixin {
    ///If specified, contains a cursor to query next page of the `items` collection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    ///Total number of items in underlying collection, fragment of which is available in `items` collection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_count: Option<i64>,
}
impl std::fmt::Display for ResultSetCursorMixin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
