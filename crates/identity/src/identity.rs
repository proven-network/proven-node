use crate::ledger_identity::LedgerIdentity;

use serde::{Deserialize, Serialize};
use webauthn_rs::prelude::Passkey;

/// A session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Identity {
    /// The identity ID.
    pub identity_id: String,

    /// Ledger identities linked to this identity.
    pub ledger_identities: Vec<LedgerIdentity>,

    /// Device passkeys for confirming important interactions.
    pub passkeys: Vec<Passkey>,
}
