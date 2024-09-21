use serde::{Serialize, Deserialize};
use super::AccountResourcePreferenceRule;
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AccountDepositPreValidationDecidingFactorsResourceSpecificDetailsItem {
    pub is_xrd: bool,
    ///Bech32m-encoded human readable version of the address.
    pub resource_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_preference_rule: Option<AccountResourcePreferenceRule>,
    pub vault_exists: bool,
}
impl std::fmt::Display
for AccountDepositPreValidationDecidingFactorsResourceSpecificDetailsItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
