use crate::error::{Error, Result};

use std::collections::HashSet;

use proven_governance::{NodeSpecialization, TopologyNode};
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
        let response = self.request_info("/nats-cluster-endpoint").await?;

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
        let response = reqwest::get(format!("{}{}", self.origin(), path)).await?;

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
