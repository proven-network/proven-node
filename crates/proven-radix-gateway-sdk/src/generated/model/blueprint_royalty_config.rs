use serde::{Serialize, Deserialize};
use super::BlueprintMethodRoyalty;
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct BlueprintRoyaltyConfig {
    pub is_enabled: bool,
    ///The royalty rules by method. The array is only present if royalties are enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method_rules: Option<Vec<BlueprintMethodRoyalty>>,
}
impl std::fmt::Display for BlueprintRoyaltyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
