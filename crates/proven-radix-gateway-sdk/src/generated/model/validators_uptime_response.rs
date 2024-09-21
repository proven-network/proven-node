use serde::{Serialize, Deserialize};
use super::{LedgerStateMixin, ValidatorUptimeCollection};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ValidatorsUptimeResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    pub validators: ValidatorUptimeCollection,
}
impl std::fmt::Display for ValidatorsUptimeResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for ValidatorsUptimeResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for ValidatorsUptimeResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
