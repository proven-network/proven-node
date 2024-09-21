use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TransactionFungibleFeeBalanceChangeType {
    FeePayment,
    FeeDistributed,
    TipDistributed,
    RoyaltyDistributed,
}
