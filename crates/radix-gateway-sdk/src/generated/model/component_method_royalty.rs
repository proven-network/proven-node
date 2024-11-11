use super::RoyaltyAmount;
use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ComponentMethodRoyalty {
    pub method_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub royalty_amount: Option<RoyaltyAmount>,
}
impl std::fmt::Display for ComponentMethodRoyalty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}