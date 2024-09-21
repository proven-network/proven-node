use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ValidatorVaultItem {
    ///Bech32m-encoded human readable version of the address.
    pub address: String,
    ///String-encoded decimal representing the amount of a related fungible resource.
    pub balance: String,
    pub last_changed_at_state_version: i64,
}
impl std::fmt::Display for ValidatorVaultItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
