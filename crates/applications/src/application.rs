use serde::{Deserialize, Serialize};

/// Represents an application.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Application {
    /// The unique identifier for the application.
    pub id: String,

    /// The address of the owner's identity.
    pub owner_identity_address: String,

    /// The addresses of the dApp definitions.
    pub dapp_definition_addresses: Vec<String>,
}
