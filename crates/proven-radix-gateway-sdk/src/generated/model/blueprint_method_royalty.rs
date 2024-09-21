use serde::{Serialize, Deserialize};
use super::RoyaltyAmount;
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct BlueprintMethodRoyalty {
    pub method_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub royalty_amount: Option<RoyaltyAmount>,
}
impl std::fmt::Display for BlueprintMethodRoyalty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
