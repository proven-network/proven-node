use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum PackageVmType {
    Native,
    ScryptoV1,
}
