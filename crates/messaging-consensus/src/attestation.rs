//! Attestation verification for consensus peers.

use std::fmt::Debug;

use bytes::Bytes;
use proven_attestation::{AttestationParams, Attestor, Pcrs, VerifiedAttestation};
use proven_governance::{Governance, Version};
use tracing::{info, warn};

use crate::error::ConsensusError;

/// Attestation verification result
pub struct PeerAttestationInfo {
    /// The verified attestation document
    pub verified_attestation: VerifiedAttestation,
    /// Whether the PCRs match a known version
    pub pcr_match: bool,
    /// The matching version (if any)
    pub matching_version: Option<Version>,
}

impl std::fmt::Debug for PeerAttestationInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerAttestationInfo")
            .field("pcr_match", &self.pcr_match)
            .field("matching_version", &self.matching_version)
            .field("verified_attestation", &"<VerifiedAttestation>")
            .finish()
    }
}

/// Attestation verifier for consensus peers
#[derive(Clone, Debug)]
pub struct AttestationVerifier<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    governance: G,
    attestor: A,
}

impl<G, A> AttestationVerifier<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    /// Create a new attestation verifier
    pub const fn new(governance: G, attestor: A) -> Self {
        Self {
            governance,
            attestor,
        }
    }

    /// Generate our own attestation document for sharing with peers
    ///
    /// # Errors
    ///
    /// Returns `ConsensusError::AttestationFailure` if the attestation generation fails.
    pub async fn generate_peer_attestation(
        &self,
        nonce: Option<Bytes>,
    ) -> Result<Bytes, ConsensusError> {
        let params = AttestationParams {
            nonce,
            user_data: Some(b"proven-consensus-peer".to_vec().into()),
            public_key: None,
        };

        self.attestor.attest(params).await.map_err(|e| {
            ConsensusError::AttestationFailure(format!("Failed to generate attestation: {e:?}"))
        })
    }

    /// Verify a peer's attestation document
    ///
    /// # Errors
    ///
    /// Returns `ConsensusError::AttestationFailure` if the attestation verification fails
    /// or `ConsensusError::GovernanceFailure` if retrieving governance versions fails.
    pub async fn verify_peer_attestation(
        &self,
        attestation_doc: Bytes,
    ) -> Result<PeerAttestationInfo, ConsensusError> {
        // First, verify the attestation document cryptographically
        let verified_attestation = self.attestor.verify(attestation_doc).map_err(|e| {
            ConsensusError::AttestationFailure(format!("Failed to verify attestation: {e:?}"))
        })?;

        info!("Successfully verified peer attestation document");

        // Get known versions from governance
        let versions = self.governance.get_active_versions().await.map_err(|e| {
            ConsensusError::GovernanceFailure(format!("Failed to get versions: {e:?}"))
        })?;

        // Check if PCRs match any known version
        let (pcr_match, matching_version) =
            Self::check_pcr_match(&verified_attestation.pcrs, &versions);

        if pcr_match {
            info!("Peer PCRs match known version: {:?}", matching_version);
        } else {
            warn!("Peer PCRs do not match any known version");
        }

        Ok(PeerAttestationInfo {
            verified_attestation,
            pcr_match,
            matching_version,
        })
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
        // For consensus messaging, we primarily care about PCR0, PCR1, and PCR2
        // which represent the kernel, application, and configuration respectively
        peer_pcrs.pcr0 == version.ne_pcr0
            && peer_pcrs.pcr1 == version.ne_pcr1
            && peer_pcrs.pcr2 == version.ne_pcr2
    }

    /// Validate that a peer is authorized to participate in consensus
    ///
    /// # Errors
    ///
    /// Returns `ConsensusError` if peer verification fails or if the peer is not authorized.
    pub async fn authorize_peer(&self, attestation_doc: Bytes) -> Result<bool, ConsensusError> {
        let attestation_info = self.verify_peer_attestation(attestation_doc).await?;

        // For consensus, we require PCR match
        if !attestation_info.pcr_match {
            warn!("Rejecting peer: PCRs do not match any known version");
            return Ok(false);
        }

        // Additional checks can be added here (e.g., user_data validation)
        if let Some(user_data) = &attestation_info.verified_attestation.user_data {
            if user_data != &b"proven-consensus-peer".to_vec() {
                warn!("Rejecting peer: invalid user_data in attestation");
                return Ok(false);
            }
        }

        info!("Peer authorized for consensus participation");
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_attestation_mock::MockAttestor;
    use proven_governance::Version;
    use proven_governance_mock::MockGovernance;

    #[tokio::test]
    async fn test_peer_attestation_verification() {
        let governance = MockGovernance::new(
            vec![],
            vec![Version {
                ne_pcr0: Bytes::from("test_pcr0"),
                ne_pcr1: Bytes::from("test_pcr1"),
                ne_pcr2: Bytes::from("test_pcr2"),
            }],
            "http://localhost:3000".to_string(),
            vec![],
        );

        let attestor = MockAttestor::new();
        let verifier = AttestationVerifier::new(governance, attestor.clone());

        // Generate a test attestation
        let attestation_doc = verifier
            .generate_peer_attestation(Some(b"test_nonce".to_vec().into()))
            .await
            .unwrap();

        // Note: This test will fail PCR match because MockAttestor generates different PCRs
        // In a real implementation, you'd configure the MockAttestor with matching PCRs
        let result = verifier
            .verify_peer_attestation(attestation_doc)
            .await
            .unwrap();

        // The verification should succeed even if PCRs don't match (cryptographic verification)
        assert!(result.verified_attestation.user_data.is_some());
    }
}
