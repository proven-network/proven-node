use crate::NATS_CLUSTER_ENDPOINT_API_PATH;
use crate::error::{Error, Result};

use std::collections::HashSet;

use bytes::Bytes;
use headers::Header;
use proven_governance::{NodeSpecialization, TopologyNode};
use proven_headers::Nonce;
use rand::RngCore;
use reqwest::header::HeaderValue;
use url::Url;

/// A node in the network.
#[derive(Clone, Debug)]
pub struct Peer(TopologyNode);

impl Peer {
    /// Get the availability zone of the node.
    pub fn availability_zone(&self) -> &str {
        &self.0.availability_zone
    }

    /// FQDN of the node.
    pub fn fqdn(&self) -> Result<String> {
        let url = Url::parse(self.origin())?;

        Ok(url.host_str().ok_or(Error::BadOrigin)?.to_string())
    }

    /// Get the nats cluster endpoint of the node.
    pub async fn nats_cluster_endpoint(&self) -> Result<String> {
        let response = self.request_info(NATS_CLUSTER_ENDPOINT_API_PATH).await?;

        Ok(response)
    }

    /// Get the origin of the node.
    pub fn origin(&self) -> &str {
        &self.0.origin
    }

    /// Get the public key of the node.
    pub fn public_key(&self) -> &str {
        &self.0.public_key
    }

    /// Get the region of the node.
    pub fn region(&self) -> &str {
        &self.0.region
    }

    /// Get the specializations of the node.
    pub fn specializations(&self) -> &HashSet<NodeSpecialization> {
        &self.0.specializations
    }

    async fn request_info(&self, path: &str) -> Result<String> {
        let mut nonce_bytes = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut nonce_bytes);

        let nonce = Nonce(Bytes::copy_from_slice(&nonce_bytes));

        let client = reqwest::Client::new();

        let response = client
            .get(format!("{}{}", self.origin(), path))
            .header(Nonce::name(), nonce.encode_to_value()?)
            .send()
            .await?;

        if response.status() != reqwest::StatusCode::OK {
            return Err(Error::RequestFailed(response.status().as_u16()));
        }

        response.text().await.map_err(Error::Reqwest)
    }
}

impl From<TopologyNode> for Peer {
    fn from(node: TopologyNode) -> Self {
        Self(node)
    }
}

trait HeaderExt {
    fn encode_to_value(&self) -> Result<HeaderValue>;
}

impl<H> HeaderExt for H
where
    H: headers::Header,
{
    fn encode_to_value(&self) -> Result<HeaderValue> {
        let mut values = Vec::new();
        self.encode(&mut values);

        values.pop().ok_or(Error::NonceEncoding)
    }
}
