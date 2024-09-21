use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum NonFungibleIdType {
    String,
    Integer,
    Bytes,
    Ruid,
}
