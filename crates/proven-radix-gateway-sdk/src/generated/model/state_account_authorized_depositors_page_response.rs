use serde::{Serialize, Deserialize};
use super::{AccountAuthorizedDepositorsCollection, LedgerStateMixin};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateAccountAuthorizedDepositorsPageResponse {
    ///Account resource preferences collection
    #[serde(flatten)]
    pub account_authorized_depositors_collection: AccountAuthorizedDepositorsCollection,
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    ///Bech32m-encoded human readable version of the address.
    pub account_address: String,
}
impl std::fmt::Display for StateAccountAuthorizedDepositorsPageResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateAccountAuthorizedDepositorsPageResponse {
    type Target = AccountAuthorizedDepositorsCollection;
    fn deref(&self) -> &Self::Target {
        &self.account_authorized_depositors_collection
    }
}
impl std::ops::DerefMut for StateAccountAuthorizedDepositorsPageResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.account_authorized_depositors_collection
    }
}
