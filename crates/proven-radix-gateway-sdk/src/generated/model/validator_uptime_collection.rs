use serde::{Serialize, Deserialize};
use super::ValidatorUptimeCollectionItem;
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ValidatorUptimeCollection {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<ValidatorUptimeCollectionItem>,
}
impl std::fmt::Display for ValidatorUptimeCollection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
