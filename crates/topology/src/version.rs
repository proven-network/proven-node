//! Version information for nodes in the topology.

use bytes::Bytes;
use proven_attestation::Pcrs;

/// A version of the node software.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Version {
    /// A contiguous measure of the contents of the image file, without the section data.
    pub ne_pcr0: Bytes,

    /// A contiguous measurement of the kernel and boot ramfs data.
    pub ne_pcr1: Bytes,

    /// A contiguous, in-order measurement of the user applications, without the boot ramfs.
    pub ne_pcr2: Bytes,
}

impl Version {
    /// Create a new from attestation PCRs.
    pub fn from_pcrs(pcrs: Pcrs) -> Self {
        Self {
            ne_pcr0: pcrs.pcr0,
            ne_pcr1: pcrs.pcr1,
            ne_pcr2: pcrs.pcr2,
        }
    }

    /// Check if the version matches the PCRs.
    pub fn matches_pcrs(&self, pcrs: &Pcrs) -> bool {
        self.ne_pcr0 == pcrs.pcr0 && self.ne_pcr1 == pcrs.pcr1 && self.ne_pcr2 == pcrs.pcr2
    }
}
