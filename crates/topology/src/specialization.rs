//! Node specialization types for the topology.

use serde::{Deserialize, Serialize};

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
