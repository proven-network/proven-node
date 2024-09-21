use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AccountLockerAddress {
    ///Bech32m-encoded human readable version of the address.
    pub account_address: String,
    ///Bech32m-encoded human readable version of the address.
    pub locker_address: String,
}
impl std::fmt::Display for AccountLockerAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
