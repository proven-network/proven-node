use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Application {
    pub id: String,
    pub owner_identity_address: String,
    pub dapp_definition_addresses: Vec<String>,
}
