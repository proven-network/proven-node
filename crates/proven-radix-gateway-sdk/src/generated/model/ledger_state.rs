use serde::{Serialize, Deserialize};
/**The ledger state against which the response was generated.
Can be used to detect if the Network Gateway is returning up-to-date information.*/
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct LedgerState {
    ///The epoch number of the ledger at this state version.
    pub epoch: i64,
    ///The logical name of the network
    pub network: String,
    /**The proposer round timestamp of the consensus round when this transaction was committed to ledger.
This is not guaranteed to be strictly increasing, as it is computed as an average across the validator set.
If this is significantly behind the current timestamp, the Network Gateway is likely reporting out-dated
information, or the network has stalled.*/
    pub proposer_round_timestamp: String,
    ///The consensus round in the epoch that this state version was committed in.
    pub round: i64,
    ///The state version of the ledger. Each transaction increments the state version by 1.
    pub state_version: i64,
}
impl std::fmt::Display for LedgerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
