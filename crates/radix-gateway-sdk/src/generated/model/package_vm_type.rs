use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum PackageVmType {
    Native,
    ScryptoV1,
}
