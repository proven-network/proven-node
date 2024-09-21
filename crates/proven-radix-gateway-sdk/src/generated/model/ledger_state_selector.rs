use serde::{Serialize, Deserialize};
/**Optional. This allows for a request to be made against a historic state.
If a constraint is specified, the Gateway will resolve the request against the ledger state at that time.
If not specified, requests will be made with respect to the top of the committed ledger.*/
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct LedgerStateSelector {
    ///If provided, the ledger state lower than or equal to the given epoch at round 0 is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<i64>,
    ///If provided must be accompanied with `epoch`, the ledger state lower than or equal to the given epoch and round is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub round: Option<i64>,
    ///If provided, the latest ledger state lower than or equal to the given state version is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_version: Option<i64>,
    ///If provided, the latest ledger state lower than or equal to the given round timestamp is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
}
impl std::fmt::Display for LedgerStateSelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
