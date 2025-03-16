//! Abstract interface for getting active version information, network topology, etc. from a governance mechanism.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use std::collections::HashSet;
use std::error::Error;
use std::fmt::Debug;
use std::time::SystemTime;

use async_trait::async_trait;

/// A node in the network topology.
#[derive(Clone, Debug, Eq, PartialEq)]
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
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum NodeSpecialization {
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

/// Marker trait for `Governance` errors
pub trait GovernanceError: Debug + Error + Send + Sync {}

/// Abstract interface for getting active version information, network topology, etc. from a governance mechanism.
#[async_trait]
pub trait Governance
where
    Self: Send + Sync + 'static,
{
    /// The error type for this server.
    type Error: GovernanceError;

    /// Get the active versions of the node.
    async fn get_active_versions(&self) -> Result<Vec<Version>, Self::Error>;

    /// Get the network topology.
    async fn get_topology(&self) -> Result<Vec<Node>, Self::Error>;
}
