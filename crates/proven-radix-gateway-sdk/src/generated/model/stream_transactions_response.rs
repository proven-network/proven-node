use serde::{Serialize, Deserialize};
use super::{CommittedTransactionInfo, LedgerStateMixin, ResultSetCursorMixin};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StreamTransactionsResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    #[serde(flatten)]
    pub result_set_cursor_mixin: ResultSetCursorMixin,
    ///The page of user transactions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub items: Vec<CommittedTransactionInfo>,
}
impl std::fmt::Display for StreamTransactionsResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StreamTransactionsResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for StreamTransactionsResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
