use serde::{Serialize, Deserialize};
use super::{LedgerStateMixin, NonFungibleIdType, StateNonFungibleDetailsResponseItem};
#[derive(Debug, Serialize, Deserialize)]
pub struct StateNonFungibleDataResponse {
    #[serde(flatten)]
    pub ledger_state_mixin: LedgerStateMixin,
    pub non_fungible_id_type: NonFungibleIdType,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub non_fungible_ids: Vec<StateNonFungibleDetailsResponseItem>,
    ///Bech32m-encoded human readable version of the address.
    pub resource_address: String,
}
impl std::fmt::Display for StateNonFungibleDataResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for StateNonFungibleDataResponse {
    type Target = LedgerStateMixin;
    fn deref(&self) -> &Self::Target {
        &self.ledger_state_mixin
    }
}
impl std::ops::DerefMut for StateNonFungibleDataResponse {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ledger_state_mixin
    }
}
