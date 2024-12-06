use serde::{Deserialize, Serialize};

/// A session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    /// The account addresses.
    pub account_addresses: Vec<String>,

    /// The dApp definition address.
    pub dapp_definition_address: String,

    /// The expected origin of future requests.
    pub expected_origin: String,

    /// The identity address.
    pub identity_address: String,

    /// The session ID.
    pub session_id: String,

    /// The node signing key.
    pub signing_key: Vec<u8>,

    /// The node verifying key.
    pub verifying_key: Vec<u8>,
}
