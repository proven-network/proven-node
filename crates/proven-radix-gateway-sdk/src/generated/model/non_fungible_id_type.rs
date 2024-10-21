use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum NonFungibleIdType {
    String,
    Integer,
    Bytes,
    Ruid,
}
