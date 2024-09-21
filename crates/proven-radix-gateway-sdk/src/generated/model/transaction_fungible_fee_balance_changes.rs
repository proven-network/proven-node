use serde::{Serialize, Deserialize};
use super::TransactionFungibleFeeBalanceChangeType;
#[derive(Debug, Serialize, Deserialize)]
pub struct TransactionFungibleFeeBalanceChanges {
    ///The string-encoded decimal representing the amount of change for the fungible resource.
    pub balance_change: String,
    ///Bech32m-encoded human readable version of the address.
    pub entity_address: String,
    ///Bech32m-encoded human readable version of the address.
    pub resource_address: String,
    /**Indicates fee-related balance changes, for example:

- payment of the fee including tip and royalty,
- distribution of royalties,
- distribution of the fee and tip to the consensus-manager, for distributing to the relevant validator/s at end of epoch.

See https://www.radixdlt.com/blog/how-fees-work-in-babylon for further information on how fee payment works at Babylon.*/
    #[serde(rename = "type")]
    pub type_: TransactionFungibleFeeBalanceChangeType,
}
impl std::fmt::Display for TransactionFungibleFeeBalanceChanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
