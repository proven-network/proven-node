use serde::{Deserialize, Serialize};

/// A Radix identity derived from ROLA.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RadixIdentityDetails {
    /// The account addresses.
    pub account_addresses: Vec<String>,

    /// The dApp definition address.
    pub dapp_definition_address: String,

    /// The expected origin of future requests.
    pub expected_origin: String,

    /// The identity address.
    pub identity_address: String,
}
