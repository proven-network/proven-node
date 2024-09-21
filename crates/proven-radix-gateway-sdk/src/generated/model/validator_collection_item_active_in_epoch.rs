use serde::{Serialize, Deserialize};
use super::PublicKey;
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ValidatorCollectionItemActiveInEpoch {
    pub key: PublicKey,
    ///String-encoded decimal representing the amount of a related fungible resource.
    pub stake: String,
    pub stake_percentage: f64,
}
impl std::fmt::Display for ValidatorCollectionItemActiveInEpoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
