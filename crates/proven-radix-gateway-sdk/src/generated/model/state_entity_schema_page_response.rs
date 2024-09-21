use serde::{Serialize, Deserialize};
use super::{EntitySchemaCollection, LedgerStateMixin};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateEntitySchemaPageResponse {
    #[serde(flatten)]
    pub entity_schema_collection: EntitySchemaCollection,
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    ///Bech32m-encoded human readable version of the address.
    pub address: String,
}
impl std::fmt::Display for StateEntitySchemaPageResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateEntitySchemaPageResponse {
    type Target = EntitySchemaCollection;
    fn deref(&self) -> &Self::Target {
        &self.entity_schema_collection
    }
}
impl std::ops::DerefMut for StateEntitySchemaPageResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entity_schema_collection
    }
}
