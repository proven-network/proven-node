use serde::{Serialize, Deserialize};
use super::{AccountResourcePreferencesCollection, LedgerStateMixin};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateAccountResourcePreferencesPageResponse {
    ///Account resource preferences collection
    #[serde(flatten)]
    pub account_resource_preferences_collection: AccountResourcePreferencesCollection,
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    ///Bech32m-encoded human readable version of the address.
    pub account_address: String,
}
impl std::fmt::Display for StateAccountResourcePreferencesPageResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateAccountResourcePreferencesPageResponse {
    type Target = AccountResourcePreferencesCollection;
    fn deref(&self) -> &Self::Target {
        &self.account_resource_preferences_collection
    }
}
impl std::ops::DerefMut for StateAccountResourcePreferencesPageResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.account_resource_preferences_collection
    }
}
