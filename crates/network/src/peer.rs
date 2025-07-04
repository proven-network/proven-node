use crate::NATS_CLUSTER_ENDPOINT_API_PATH;
use crate::error::{Error, Result};

use std::collections::HashSet;

use bytes::Bytes;
use ed25519_dalek::VerifyingKey;
use headers::Header;
use proven_attestation::Attestor;
use proven_governance::{GovernanceNode, NodeSpecialization, Version};
use proven_headers::{Attestation, Nonce};
use rand::RngCore;
use reqwest::header::{HeaderName as ReqwestHeaderName, HeaderValue as ReqwestHeaderValue};
use tracing::warn;
use url::Url;

/// A node in the network.
#[derive(Clone, Debug)]
pub struct Peer<A>
where
    A: Attestor,
{
    attestor: A,
    node: GovernanceNode,
    versions: Vec<Version>,
}

impl<A> Peer<A>
where
    A: Attestor,
{
    /// Create a new Peer instance.
    pub const fn new(node: GovernanceNode, versions: Vec<Version>, attestor: A) -> Self {
        Self {
            attestor,
            node,
            versions,
        }
    }

    /// Get the availability zone of the node.
    #[allow(clippy::missing_const_for_fn)]
    pub fn availability_zone(&self) -> &str {
        &self.node.availability_zone
    }

    /// FQDN of the node.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to parse origin
    /// - Failed to get host string from URL
    pub fn fqdn(&self) -> Result<String> {
        let url = Url::parse(self.origin())?;

        Ok(url.host_str().ok_or(Error::BadOrigin)?.to_string())
    }

    /// Get the nats cluster endpoint of the node.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to request info
    pub async fn nats_cluster_endpoint(&self) -> Result<String> {
        let response = self.request_info(NATS_CLUSTER_ENDPOINT_API_PATH).await?;

        Ok(response)
    }

    /// Get the origin of the node.
    #[allow(clippy::missing_const_for_fn)]
    pub fn origin(&self) -> &str {
        &self.node.origin
    }

    /// Get the public key of the node.
    pub const fn public_key(&self) -> &VerifyingKey {
        &self.node.public_key
    }

    /// Get the region of the node.
    #[allow(clippy::missing_const_for_fn)]
    pub fn region(&self) -> &str {
        &self.node.region
    }

    /// Get the specializations of the node.
    pub const fn specializations(&self) -> &HashSet<NodeSpecialization> {
        &self.node.specializations
    }

    async fn request_info(&self, path: &str) -> Result<String> {
        let mut nonce_bytes = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce(Bytes::copy_from_slice(&nonce_bytes));
        let (nonce_header_name, nonce_header_value) = nonce.encode_to_reqwest_header_tuple()?;

        let client = reqwest::Client::new();
        let response = client
            .get(format!("{}{}", self.origin(), path))
            .header(nonce_header_name, nonce_header_value)
            .send()
            .await?;

        if response.status() != reqwest::StatusCode::OK {
            return Err(Error::RequestFailed(response.status().as_u16()));
        }

        let headers = response.headers();
        let attestation_value = headers
            .get(Attestation::name())
            .ok_or(Error::MissingHeader("X-Attestation"))?;

        let mut values = std::iter::once(attestation_value);
        let attestation = Attestation::decode(&mut values)
            .map_err(|_e| Error::InvalidHeader("Failed to decode X-Attestation"))?;

        let verified_attestation = self
            .attestor
            .verify(attestation.0)
            .map_err(|e| Error::AttestationVerificationFailed(e.to_string()))?;

        // Check nonce matches
        let expected_nonce = Bytes::copy_from_slice(&nonce_bytes);
        if verified_attestation.nonce.as_ref() != Some(&expected_nonce) {
            warn!(
                "Nonce mismatch: sent={:?}, received={:?}",
                hex::encode(&expected_nonce),
                verified_attestation.nonce.as_ref().map(hex::encode)
            );
            return Err(Error::NonceMismatch);
        }

        // Check PCRs match known version
        let pcrs = verified_attestation.pcrs;
        self.versions
            .iter()
            .find(|v| v.matches_pcrs(&pcrs))
            .ok_or(Error::VersionMismatch)?;

        response.text().await.map_err(Error::Reqwest)
    }
}

trait HeaderExt {
    fn encode_to_reqwest_header_tuple(&self) -> Result<(ReqwestHeaderName, ReqwestHeaderValue)>;
}

impl<H> HeaderExt for H
where
    H: Header,
{
    fn encode_to_reqwest_header_tuple(&self) -> Result<(ReqwestHeaderName, ReqwestHeaderValue)> {
        let mut values = Vec::<headers::HeaderValue>::new();
        self.encode(&mut values);

        let header_value = values.pop().ok_or(Error::NonceEncoding)?;

        let header_name =
            ReqwestHeaderName::from_bytes(H::name().as_ref()).map_err(|_| Error::NonceEncoding)?;

        let reqwest_value = ReqwestHeaderValue::from_bytes(header_value.as_bytes())
            .map_err(|_| Error::NonceEncoding)?;

        Ok((header_name, reqwest_value))
    }
}
