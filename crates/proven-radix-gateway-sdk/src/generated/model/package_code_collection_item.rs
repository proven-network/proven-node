use serde::{Serialize, Deserialize};
use super::PackageVmType;
#[derive(Debug, Serialize, Deserialize)]
pub struct PackageCodeCollectionItem {
    ///Hex-encoded binary blob.
    pub code_hash_hex: String,
    ///Hex-encoded binary blob.
    pub code_hex: String,
    pub vm_type: PackageVmType,
}
impl std::fmt::Display for PackageCodeCollectionItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}
