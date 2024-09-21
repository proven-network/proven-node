use serde::{Serialize, Deserialize};
use super::{LedgerStateMixin, ValidatorCollection};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateValidatorsListResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    pub validators: ValidatorCollection,
}
impl std::fmt::Display for StateValidatorsListResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateValidatorsListResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for StateValidatorsListResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
