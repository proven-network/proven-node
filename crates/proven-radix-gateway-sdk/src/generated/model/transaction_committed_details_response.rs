use serde::{Serialize, Deserialize};
use super::{CommittedTransactionInfo, LedgerStateMixin};
#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionCommittedDetailsResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    pub transaction: CommittedTransactionInfo,
}
impl std::fmt::Display for TransactionCommittedDetailsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for TransactionCommittedDetailsResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for TransactionCommittedDetailsResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
