use serde::{Serialize, Deserialize};
use super::{
    TransactionFungibleBalanceChanges, TransactionFungibleFeeBalanceChanges,
    TransactionNonFungibleBalanceChanges,
};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TransactionBalanceChanges {
    ///A list of all non-fee-related fungible balance changes per entity and resource.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fungible_balance_changes: Vec<TransactionFungibleBalanceChanges>,
    ///A list of all fee-related fungible balance changes per entity and resource.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fungible_fee_balance_changes: Vec<TransactionFungibleFeeBalanceChanges>,
    ///A list of all non-fungible changes per entity and resource.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub non_fungible_balance_changes: Vec<TransactionNonFungibleBalanceChanges>,
}
impl std::fmt::Display for TransactionBalanceChanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
