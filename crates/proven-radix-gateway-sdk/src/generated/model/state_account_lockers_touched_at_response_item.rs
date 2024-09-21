use serde::{Serialize, Deserialize};
use super::AccountLockerAddress;
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateAccountLockersTouchedAtResponseItem {
    #[serde(flatten)]
    pub account_locker_address: AccountLockerAddress,
    ///The most recent state version underlying object was modified at.
    pub last_touched_at_state_version: i64,
}
impl std::fmt::Display for StateAccountLockersTouchedAtResponseItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateAccountLockersTouchedAtResponseItem {
    type Target = AccountLockerAddress;
    fn deref(&self) -> &Self::Target {
        &self.account_locker_address
    }
}
impl std::ops::DerefMut for StateAccountLockersTouchedAtResponseItem {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.account_locker_address
    }
}
