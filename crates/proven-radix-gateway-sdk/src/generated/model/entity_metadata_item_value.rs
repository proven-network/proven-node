use serde::{Serialize, Deserialize};
use super::{MetadataTypedValue, ScryptoSborValue};
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct EntityMetadataItemValue {
    #[serde(flatten)]
    pub scrypto_sbor_value: ScryptoSborValue,
    pub typed: MetadataTypedValue,
}
impl std::fmt::Display for EntityMetadataItemValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
impl std::ops::Deref for EntityMetadataItemValue {
    type Target = ScryptoSborValue;
    fn deref(&self) -> &Self::Target {
        &self.scrypto_sbor_value
    }
}
impl std::ops::DerefMut for EntityMetadataItemValue {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.scrypto_sbor_value
    }
}
