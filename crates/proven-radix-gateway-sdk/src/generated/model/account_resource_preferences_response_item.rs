use serde::{Serialize, Deserialize};
use super::AccountResourcePreferenceRule;
#[derive(Debug, Serialize, Deserialize)]
pub struct AccountResourcePreferencesResponseItem {
    ///The most recent state version underlying object was modified at.
    pub last_updated_at_state_version: i64,
    ///Bech32m-encoded human readable version of the address.
    pub resource_address: String,
    pub resource_preference_rule: AccountResourcePreferenceRule,
}
impl std::fmt::Display for AccountResourcePreferencesResponseItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
