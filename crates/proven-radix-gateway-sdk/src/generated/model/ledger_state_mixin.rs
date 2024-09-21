use serde::{Serialize, Deserialize};
use super::LedgerState;
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct LedgerStateMixin {
    /**The ledger state against which the response was generated.
Can be used to detect if the Network Gateway is returning up-to-date information.*/
    pub ledger_state: LedgerState,
}
impl std::fmt::Display for LedgerStateMixin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
