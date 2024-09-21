use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TransactionFungibleBalanceChanges {
    ///The string-encoded decimal representing the amount of change for the fungible resource.
    pub balance_change: String,
    ///Bech32m-encoded human readable version of the address.
    pub entity_address: String,
    ///Bech32m-encoded human readable version of the address.
    pub resource_address: String,
}
impl std::fmt::Display for TransactionFungibleBalanceChanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
