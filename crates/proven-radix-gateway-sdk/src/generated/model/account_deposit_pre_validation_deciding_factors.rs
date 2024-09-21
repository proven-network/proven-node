use serde::{Serialize, Deserialize};
use super::{
    AccountDefaultDepositRule,
    AccountDepositPreValidationDecidingFactorsResourceSpecificDetailsItem,
};
///Deciding factors used to calculate response.
#[derive(Debug, Serialize, Deserialize)]
pub struct AccountDepositPreValidationDecidingFactors {
    ///Default deposit rule set to an account.
    pub default_deposit_rule: AccountDefaultDepositRule,
    ///Whether the input badge belongs to the account's set of authorized depositors. This field will only be present if any badge was passed in the request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_badge_authorized_depositor: Option<bool>,
    ///Returns deciding factors for each resource. Contains only information about resources presented in the request, not all resource preference rules for queried account.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_specific_details: Option<
        Vec<AccountDepositPreValidationDecidingFactorsResourceSpecificDetailsItem>,
    >,
}
impl std::fmt::Display for AccountDepositPreValidationDecidingFactors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
