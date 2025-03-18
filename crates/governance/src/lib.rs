//! Abstract interface for getting active version information, network topology, etc. from a governance mechanism.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use std::error::Error;
use std::fmt::{self, Debug};
use std::time::SystemTime;
use std::{collections::HashSet, fmt::Display};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A node in the network topology.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Node {
    /// The availability zone of the node.
    pub availability_zone: String,

    /// The fully qualified domain name of the node.
    pub fqdn: String,

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
    /// The timestamp when the version was activated.
    pub activated_at: SystemTime,

    /// A contiguous measure of the contents of the image file, without the section data.
    pub ne_pcr0: String,

    /// A contiguous measurement of the kernel and boot ramfs data.
    pub ne_pcr1: String,

    /// A contiguous, in-order measurement of the user applications, without the boot ramfs.
    pub ne_pcr2: String,

    /// Sequence number of the version.
    pub sequence: u64,
}

/// The kind of governance error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GovernanceErrorKind {
    /// Error related to private key operations
    PrivateKey,

    /// Error when a required value is not initialized
    NotInitialized,

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

    /// Get the network topology.
    async fn get_topology(&self) -> Result<Vec<Node>, Self::Error>;

    /// Get the node definition for this node based on the private key.
    async fn get_self(&self) -> Result<Node, Self::Error>;
}
