use serde::{Serialize, Deserialize};
use super::{LedgerStateMixin, PackageCodeCollection};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StatePackageCodePageResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    ///Package code collection.
    #[serde(flatten)]
    pub package_code_collection: PackageCodeCollection,
    ///Bech32m-encoded human readable version of the address.
    pub package_address: String,
}
impl std::fmt::Display for StatePackageCodePageResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StatePackageCodePageResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for StatePackageCodePageResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
