//! Attestation verification for secure peer authentication

use bytes::Bytes;
use proven_attestation::{AttestationParams, Attestor, Pcrs};
use proven_topology::{TopologyAdaptor, Version};
use std::{fmt::Debug, sync::Arc};
use tracing::warn;

use crate::verification::error::{VerificationError, VerificationResult};

/// Handles attestation verification for peers
pub struct AttestationVerifier<G, A>
where
    G: TopologyAdaptor + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    governance: Arc<G>,
    attestor: Arc<A>,
}

impl<G, A> AttestationVerifier<G, A>
where
    G: TopologyAdaptor + Send + Sync + 'static + Debug,
    A: Attestor + Send + Sync + 'static + Debug,
{
    /// Create a new attestation verifier
    pub fn new(governance: Arc<G>, attestor: Arc<A>) -> Self {
        Self {
            governance,
            attestor,
        }
    }

    /// Verify attestation from a peer and determine if they should be authorized
    pub async fn authorize_peer(&self, attestation: Bytes) -> VerificationResult<bool> {
        // First, verify the attestation document cryptographically
        let verified_attestation = self.attestor.verify(attestation).map_err(|e| {
            VerificationError::Attestation(format!("Failed to verify attestation: {e:?}"))
        })?;

        // Get allowed versions from governance
        let allowed_versions = self.governance.get_active_versions().await.map_err(|e| {
            VerificationError::Topology(format!("Failed to get allowed versions: {e}"))
        })?;

        // Check if PCRs match any known version
        let (pcr_match, _matching_version) =
            Self::check_pcr_match(&verified_attestation.pcrs, &allowed_versions);

        // For consensus, we require PCR match
        if !pcr_match {
            warn!("Rejecting peer: PCRs do not match any known version");
            return Ok(false);
        }

        // Check user data to ensure it's a consensus peer
        if let Some(user_data) = &verified_attestation.user_data
            && user_data != &b"proven-consensus-peer".to_vec()
        {
            warn!("Rejecting peer: invalid user_data in attestation");
            return Ok(false);
        }

        Ok(true)
    }

    /// Generate our own attestation document
    pub async fn generate_peer_attestation(
        &self,
        nonce: Option<Bytes>,
    ) -> VerificationResult<Bytes> {
        let params = AttestationParams {
            nonce,
            user_data: Some(b"proven-consensus-peer".to_vec().into()),
            public_key: None,
        };

        let attestation = self.attestor.attest(params).await.map_err(|e| {
            VerificationError::Attestation(format!("Failed to generate attestation: {e:?}"))
        })?;

        Ok(attestation)
    }

    /// Get reference to governance system
    pub fn governance(&self) -> Arc<G> {
        self.governance.clone()
    }

    /// Get reference to attestor
    pub fn attestor(&self) -> Arc<A> {
        self.attestor.clone()
    }

    /// Check if PCRs match any known version
    fn check_pcr_match(peer_pcrs: &Pcrs, versions: &[Version]) -> (bool, Option<Version>) {
        for version in versions {
            if Self::pcrs_match(peer_pcrs, version) {
                return (true, Some(version.clone()));
            }
        }
        (false, None)
    }

    /// Check if PCRs match a specific version
    fn pcrs_match(peer_pcrs: &Pcrs, version: &Version) -> bool {
        // For messaging, we primarily care about PCR0, PCR1, and PCR2
        // which represent the kernel, application, and configuration respectively
        peer_pcrs.pcr0 == version.ne_pcr0
            && peer_pcrs.pcr1 == version.ne_pcr1
            && peer_pcrs.pcr2 == version.ne_pcr2
    }
}

impl<G, A> Debug for AttestationVerifier<G, A>
where
    G: TopologyAdaptor + Send + Sync + 'static + Debug,
    A: Attestor + Send + Sync + 'static + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttestationVerifier")
            .field("governance", &self.governance)
            .field("attestor", &self.attestor)
            .finish()
    }
}

impl<G, A> Clone for AttestationVerifier<G, A>
where
    G: TopologyAdaptor + Send + Sync + 'static + Debug + Clone,
    A: Attestor + Send + Sync + 'static + Debug + Clone,
{
    fn clone(&self) -> Self {
        Self {
            governance: self.governance.clone(),
            attestor: self.attestor.clone(),
        }
    }
}
