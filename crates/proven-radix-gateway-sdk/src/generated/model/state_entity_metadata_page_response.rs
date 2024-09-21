use serde::{Serialize, Deserialize};
use super::{EntityMetadataCollection, LedgerStateMixin};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateEntityMetadataPageResponse {
    ///Entity metadata collection.
    #[serde(flatten)]
    pub entity_metadata_collection: EntityMetadataCollection,
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    ///Bech32m-encoded human readable version of the address.
    pub address: String,
}
impl std::fmt::Display for StateEntityMetadataPageResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateEntityMetadataPageResponse {
    type Target = EntityMetadataCollection;
    fn deref(&self) -> &Self::Target {
        &self.entity_metadata_collection
    }
}
impl std::ops::DerefMut for StateEntityMetadataPageResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entity_metadata_collection
    }
}
