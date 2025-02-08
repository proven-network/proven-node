pub mod radix;

use radix::RadixIdentityDetails;

use serde::{Deserialize, Serialize};

/// A session-linked identity.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum LedgerIdentity {
    /// A Radix identity derived from ROLA.
    Radix(RadixIdentityDetails),
}
