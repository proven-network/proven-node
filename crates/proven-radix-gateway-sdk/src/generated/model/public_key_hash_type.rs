use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum PublicKeyHashType {
    #[serde(rename = "EcdsaSecp256k1")]
    EcdsaSecp256K1,
    EddsaEd25519,
}
