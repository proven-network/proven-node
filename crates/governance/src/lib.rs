//! Abstract interface for getting active version information, network topology, etc. from a governance mechanism.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use std::error::Error;
use std::fmt::{self, Debug};
use std::{collections::HashSet, fmt::Display};

use async_trait::async_trait;
use bytes::Bytes;
use proven_attestation::Pcrs;
use serde::{Deserialize, Serialize};

/// A node in the network topology.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TopologyNode {
    /// The availability zone of the node.
    pub availability_zone: String,

    /// The origin of the node.
    pub origin: String,

    /// The public key of the node.
    pub public_key: String,

    /// The region of the node.
    pub region: String,

    /// Any specializations of the node.
    pub specializations: HashSet<NodeSpecialization>,
}

/// The possible specializations of a node.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum NodeSpecialization {
    /// Runs a Bitcoin node.
    BitcoinMainnet,

    /// Runs a Bitcoin testnet node.
    BitcoinTestnet,

    /// Runs a Holesky (testnet) Ethereum node.
    EthereumHolesky,

    /// Runs a mainnet Ethereum node.
    EthereumMainnet,

    /// Runs a Sepolia (testnet) Ethereum node.
    EthereumSepolia,

    /// Runs a mainnet Radix node.
    RadixMainnet,

    /// Runs a stokenet (testnet) Radix node.
    RadixStokenet,
}

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

/// The kind of governance error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GovernanceErrorKind {
    /// Error when a node is not found in the topology
    NodeNotFound,

    /// Error with contract/external service
    External,

    /// Other/unknown error
    Other,
}

impl Display for GovernanceErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Marker trait for `Governance` errors
pub trait GovernanceError: Debug + Error + Send + Sync {
    /// Returns the kind of this error
    fn kind(&self) -> GovernanceErrorKind;
}

/// Abstract interface for getting active version information, network topology, etc. from a governance mechanism.
#[async_trait]
pub trait Governance
where
    Self: Send + Sync + Clone + 'static,
{
    /// The error type for this server.
    type Error: GovernanceError;

    /// Get the active versions of the node.
    async fn get_active_versions(&self) -> Result<Vec<Version>, Self::Error>;

    /// Get the alternates auth gateways.
    async fn get_alternates_auth_gateways(&self) -> Result<Vec<String>, Self::Error>;

    /// Get the primary auth gateway.
    async fn get_primary_auth_gateway(&self) -> Result<String, Self::Error>;

    /// Get the network topology.
    async fn get_topology(&self) -> Result<Vec<TopologyNode>, Self::Error>;
}
