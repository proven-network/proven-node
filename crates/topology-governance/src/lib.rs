//! Helios light-client based implementation of the topology adaptor interface.
#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;

use ed25519_dalek::VerifyingKey;
use error::Error;

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::vec::Vec;

use alloy::eips::BlockNumberOrTag;
use alloy::primitives::Address;
use alloy::rpc::types::TransactionRequest;
use alloy::sol_types::{SolCall, sol};
use async_trait::async_trait;
use helios_ethereum::{EthereumClient, EthereumClientBuilder, config::networks::Network};
use proven_topology::{Node, NodeId, NodeSpecialization, TopologyAdaptor, Version};

/// Configuration options for the Helios topology adaptor client.
#[derive(Clone, Debug)]
pub struct GovernanceTopologyAdaptorOptions {
    /// The URL of the Ethereum consensus layer RPC endpoint.
    pub consensus_rpc: String,

    /// The directory where Helios should store its data.
    pub data_dir: PathBuf,

    /// The URL of the Ethereum execution layer RPC endpoint.
    pub execution_rpc: String,

    /// The network to use.
    pub network: Network,

    /// The address of the node governance contract.
    pub node_governance_contract_address: String,

    /// The address of the token contract.
    pub token_contract_address: String,

    /// The address of the version governance contract.
    pub version_governance_contract_address: String,
}

// Define Solidity structs for smart contract interactions
sol! {
    #[derive(Debug)]
    struct VersionStruct {
        uint64 sequence;
        uint256 activatedAt;
        string nePcr0;
        string nePcr1;
        string nePcr2;
        bool active;
    }

    #[derive(Debug)]
    struct NodeStruct {
        string id;
        string region;
        string availabilityZone;
        string origin;
        string publicKey;
        bytes32[] specializations;
        address owner;
    }

    // Define function calls
    function getActiveVersions() public view returns (VersionStruct[] memory);
    function getNodes() public view returns (NodeStruct[] memory);
}

/// A topology adaptor client that uses Helios to interact with the Ethereum network.
#[derive(Clone)]
pub struct GovernanceTopologyAdaptor {
    client: Arc<EthereumClient>,
    node_governance_address: Address,
    version_governance_address: Address,
}

impl GovernanceTopologyAdaptor {
    /// Create a new Helios topology adaptor client.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The Helios client fails to build or start
    /// - The provided contract addresses cannot be parsed
    pub async fn new(options: GovernanceTopologyAdaptorOptions) -> Result<Self, Error> {
        let client = EthereumClientBuilder::new()
            .network(options.network)
            .data_dir(options.data_dir.clone())
            .consensus_rpc(&options.consensus_rpc)?
            .execution_rpc(&options.execution_rpc)?
            .load_external_fallback()
            .with_file_db()
            .build()
            .map_err(Error::Helios)?;

        // Wait for sync before wrapping in Arc
        client.wait_synced().await;

        // Now wrap in Arc after initialization is complete
        let client = Arc::new(client);

        // Parse contract addresses
        let node_governance_address = options
            .node_governance_contract_address
            .parse::<Address>()
            .map_err(|e| {
                Error::InvalidAddress(format!(
                    "Error parsing node_governance_contract_address: {e}"
                ))
            })?;

        let version_governance_address = options
            .version_governance_contract_address
            .parse::<Address>()
            .map_err(|e| {
                Error::InvalidAddress(format!(
                    "Error parsing version_governance_contract_address: {e}"
                ))
            })?;

        Ok(Self {
            client,
            node_governance_address,
            version_governance_address,
        })
    }

    /// Call a smart contract function.
    async fn call_contract_function(&self, to: Address, selector: &[u8]) -> Result<Vec<u8>, Error> {
        // Create transaction request
        let tx = TransactionRequest {
            from: None,
            to: Some(to.into()),
            gas: None,
            gas_price: None,
            value: None,
            input: selector.to_vec().into(),
            ..Default::default()
        };

        // Execute call
        let result = self
            .client
            .call(&tx, BlockNumberOrTag::Latest.into())
            .await
            .map_err(Error::Helios)?;

        Ok(result.to_vec())
    }
}

#[async_trait]
impl TopologyAdaptor for GovernanceTopologyAdaptor {
    type Error = Error;

    async fn get_active_versions(&self) -> Result<Vec<Version>, Self::Error> {
        // Prepare the call data
        let selector = getActiveVersionsCall::SELECTOR;
        let result = self
            .call_contract_function(self.version_governance_address, &selector)
            .await?;

        let versions_result = getActiveVersionsCall::abi_decode_returns(&result)
            .map_err(|e| Error::ContractDataDecode(format!("Error decoding versions: {e}")))?;

        // Extract the version structs from the result
        let mut versions = Vec::new();

        // getActiveVersions returns VersionStruct[] memory, so versions_result._0 is the array
        for version_struct in &versions_result {
            if version_struct.active {
                versions.push(Version {
                    ne_pcr0: version_struct.nePcr0.clone().into(),
                    ne_pcr1: version_struct.nePcr1.clone().into(),
                    ne_pcr2: version_struct.nePcr2.clone().into(),
                });
            }
        }

        Ok(versions)
    }

    async fn get_alternates_auth_gateways(&self) -> Result<Vec<String>, Self::Error> {
        todo!()
    }

    async fn get_primary_auth_gateway(&self) -> Result<String, Self::Error> {
        todo!()
    }

    async fn get_topology(&self) -> Result<Vec<Node>, Self::Error> {
        // Prepare the call data
        let selector = getNodesCall::SELECTOR;
        let result = self
            .call_contract_function(self.node_governance_address, &selector)
            .await?;

        let nodes_result = getNodesCall::abi_decode_returns(&result)
            .map_err(|e| Error::ContractDataDecode(format!("Error decoding nodes: {e}")))?;

        // Extract the node structs from the result
        let mut nodes = Vec::new();

        // getNodes returns NodeStruct[] memory, so nodes_result._0 is the array
        for node_struct in &nodes_result {
            let mut specializations = HashSet::new();

            // Parse specializations from bytes32 array
            for spec_bytes in &node_struct.specializations {
                // Convert the bytes32 to a string representation
                let spec_str = format!("{spec_bytes:x}");

                // Match the hash to known specializations
                // Note: In a real implementation, you would use a mapping from hash to specialization
                if spec_str.ends_with("01") {
                    specializations.insert(NodeSpecialization::BitcoinMainnet);
                } else if spec_str.ends_with("02") {
                    specializations.insert(NodeSpecialization::BitcoinTestnet);
                } else if spec_str.ends_with("03") {
                    specializations.insert(NodeSpecialization::RadixMainnet);
                } else if spec_str.ends_with("04") {
                    specializations.insert(NodeSpecialization::RadixStokenet);
                } else if spec_str.ends_with("05") {
                    specializations.insert(NodeSpecialization::EthereumMainnet);
                } else if spec_str.ends_with("06") {
                    specializations.insert(NodeSpecialization::EthereumSepolia);
                } else if spec_str.ends_with("07") {
                    specializations.insert(NodeSpecialization::EthereumHolesky);
                }
            }

            // TODO: Don't panic
            let public_key_bytes: [u8; 32] = hex::decode(node_struct.publicKey.clone())
                .unwrap()
                .try_into()
                .unwrap();
            let public_key = VerifyingKey::from_bytes(&public_key_bytes).unwrap();

            nodes.push(Node::new(
                node_struct.availabilityZone.clone(),
                node_struct.origin.clone(),
                NodeId::from(public_key),
                node_struct.region.clone(),
                specializations,
            ));
        }

        Ok(nodes)
    }
}

impl std::fmt::Debug for GovernanceTopologyAdaptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HeliosTopologyAdaptor")?;
        write!(
            f,
            "node_governance_address: {}",
            self.node_governance_address
        )?;
        write!(
            f,
            "version_governance_address: {}",
            self.version_governance_address
        )?;
        Ok(())
    }
}
