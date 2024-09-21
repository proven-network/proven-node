use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TransactionDetailsOptIns {
    ///if set to `true`, all affected global entities by given transaction are returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affected_global_entities: Option<bool>,
    /**if set to `true`, returns the fungible and non-fungible balance changes.

**Warning!** This opt-in might be missing for recently committed transactions, in that case a `null` value will be returned. Retry the request until non-null value is returned.*/
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub balance_changes: Option<bool>,
    ///if set to `true`, manifest instructions for user transactions are returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_instructions: Option<bool>,
    ///if set to `true`, raw transaction hex is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_hex: Option<bool>,
    ///if set to `true`, costing parameters inside receipt object is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_costing_parameters: Option<bool>,
    ///if set to `true`, events inside receipt object is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_events: Option<bool>,
    ///if set to `true`, fee destination inside receipt object is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_fee_destination: Option<bool>,
    ///if set to `true`, fee source inside receipt object is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_fee_source: Option<bool>,
    ///if set to `true`, fee summary inside receipt object is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_fee_summary: Option<bool>,
    ///(true by default) if set to `true`, transaction receipt output is returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_output: Option<bool>,
    ///if set to `true`, state changes inside receipt object are returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_state_changes: Option<bool>,
}
impl std::fmt::Display for TransactionDetailsOptIns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
