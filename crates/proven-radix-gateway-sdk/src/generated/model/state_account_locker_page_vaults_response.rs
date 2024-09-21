use serde::{Serialize, Deserialize};
use super::{AccountLockerAddress, AccountLockerVaultCollection, LedgerStateMixin};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateAccountLockerPageVaultsResponse {
    #[serde(flatten)]
    pub account_locker_address: AccountLockerAddress,
    ///Account Locker Account Resources Collection
    #[serde(flatten)]
    pub account_locker_vault_collection: AccountLockerVaultCollection,
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
}
impl std::fmt::Display for StateAccountLockerPageVaultsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateAccountLockerPageVaultsResponse {
    type Target = AccountLockerAddress;
    fn deref(&self) -> &Self::Target {
        &self.account_locker_address
    }
}
impl std::ops::DerefMut for StateAccountLockerPageVaultsResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.account_locker_address
    }
}
