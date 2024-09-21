use serde::{Serialize, Deserialize};
use super::{
    AccountDepositPreValidationDecidingFactors,
    AccountDepositPreValidationResourceSpecificBehaviourItem, LedgerStateMixin,
};
#[derive(Debug, Serialize, Deserialize)]
pub struct AccountDepositPreValidationResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    pub allows_try_deposit_batch: bool,
    ///Deciding factors used to calculate response.
    pub deciding_factors: AccountDepositPreValidationDecidingFactors,
    ///The fully resolved try_deposit_* ability of each resource (which takes all the inputs into account, including the authorized depositor badge, the default deposit rule and the resource-specific details).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resource_specific_behaviour: Vec<
        AccountDepositPreValidationResourceSpecificBehaviourItem,
    >,
}
impl std::fmt::Display for AccountDepositPreValidationResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for AccountDepositPreValidationResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for AccountDepositPreValidationResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
