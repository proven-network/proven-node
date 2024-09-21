use serde::{Serialize, Deserialize};
use super::{LedgerStateMixin, PackageBlueprintCollection};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StatePackageBlueprintPageResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    ///Package blueprint collection.
    #[serde(flatten)]
    pub package_blueprint_collection: PackageBlueprintCollection,
    ///Bech32m-encoded human readable version of the address.
    pub package_address: String,
}
impl std::fmt::Display for StatePackageBlueprintPageResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StatePackageBlueprintPageResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for StatePackageBlueprintPageResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
