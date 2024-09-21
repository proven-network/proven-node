use serde::{Serialize, Deserialize};
use super::{LedgerStateMixin, StateEntityDetailsResponseItem};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateEntityDetailsResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<StateEntityDetailsResponseItem>,
}
impl std::fmt::Display for StateEntityDetailsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateEntityDetailsResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for StateEntityDetailsResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
