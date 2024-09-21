use serde::{Serialize, Deserialize};
use super::{GatewayInfoResponseReleaseInfo, LedgerStateMixin};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct GatewayStatusResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    pub release_info: GatewayInfoResponseReleaseInfo,
}
impl std::fmt::Display for GatewayStatusResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for GatewayStatusResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for GatewayStatusResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
