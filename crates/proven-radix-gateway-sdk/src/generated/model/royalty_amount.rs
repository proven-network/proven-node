use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RoyaltyAmount {
    ///String-encoded decimal representing the amount of a related fungible resource.
    pub amount: String,
    pub unit: String,
}
impl std::fmt::Display for RoyaltyAmount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
