use serde::{Serialize, Deserialize};
use super::{
    EntityMetadataCollection, FungibleResourcesCollection,
    NonFungibleResourcesCollection, StateEntityDetailsResponseItemAncestorIdentities,
    StateEntityDetailsResponseItemDetails,
};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StateEntityDetailsResponseItem {
    ///Bech32m-encoded human readable version of the address.
    pub address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ancestor_identities: Option<StateEntityDetailsResponseItemAncestorIdentities>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<StateEntityDetailsResponseItemDetails>,
    ///Entity metadata collection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub explicit_metadata: Option<EntityMetadataCollection>,
    ///Fungible resources collection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fungible_resources: Option<FungibleResourcesCollection>,
    ///Entity metadata collection.
    pub metadata: EntityMetadataCollection,
    ///Non-fungible resources collection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub non_fungible_resources: Option<NonFungibleResourcesCollection>,
}
impl std::fmt::Display for StateEntityDetailsResponseItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
