use serde::{Serialize, Deserialize};
use super::{
    EntityMetadataCollection, ValidatorCollectionItemActiveInEpoch, ValidatorVaultItem,
};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ValidatorCollectionItem {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_in_epoch: Option<ValidatorCollectionItemActiveInEpoch>,
    ///Bech32m-encoded human readable version of the address.
    pub address: String,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub effective_fee_factor: serde_json::Value,
    pub locked_owner_stake_unit_vault: ValidatorVaultItem,
    ///Entity metadata collection.
    pub metadata: EntityMetadataCollection,
    pub pending_owner_stake_unit_unlock_vault: ValidatorVaultItem,
    pub pending_xrd_withdraw_vault: ValidatorVaultItem,
    pub stake_vault: ValidatorVaultItem,
    /**Validator inner state representation.
This type is defined in the Core API as `ValidatorFieldStateValue`. See the Core API documentation for more details.*/
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<serde_json::Value>,
}
impl std::fmt::Display for ValidatorCollectionItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
