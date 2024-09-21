use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ValidatorUptimeCollectionItem {
    ///Bech32m-encoded human readable version of the address.
    pub address: String,
    ///number of epochs validator was active in.
    pub epochs_active_in: i64,
    ///number of proposals made.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposals_made: Option<i64>,
    ///number of proposals missed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposals_missed: Option<i64>,
}
impl std::fmt::Display for ValidatorUptimeCollectionItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
