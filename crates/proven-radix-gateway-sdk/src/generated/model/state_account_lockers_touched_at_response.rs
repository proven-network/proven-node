use serde::{Serialize, Deserialize};
use super::{LedgerStateMixin, StateAccountLockersTouchedAtResponseItem};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateAccountLockersTouchedAtResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<StateAccountLockersTouchedAtResponseItem>,
}
impl std::fmt::Display for StateAccountLockersTouchedAtResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateAccountLockersTouchedAtResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for StateAccountLockersTouchedAtResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
