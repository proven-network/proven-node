//! Helios light-client based implementation of the governance interface.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;

use error::Error;

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};
use std::vec::Vec;

use alloy::primitives::Address;
use alloy::rpc::types::TransactionRequest;
use alloy_sol_types::{sol, SolCall};
use async_trait::async_trait;
use helios_common::types::BlockTag;
use helios_ethereum::{
    config::networks::Network, database::FileDB, EthereumClient, EthereumClientBuilder,
};
use proven_governance::{Governance, Node, NodeSpecialization, Version};

/// Configuration options for the Helios governance client.
#[derive(Clone, Debug)]
pub struct HeliosGovernanceOptions {
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
        string fqdn;
        string publicKey;
        bytes32[] specializations;
        address owner;
    }

    // Define function calls
    function getActiveVersions() public view returns (VersionStruct[] memory);
    function getNodes() public view returns (NodeStruct[] memory);
}

/// A governance client that uses Helios to interact with the Ethereum network.
pub struct HeliosGovernance {
    client: Arc<EthereumClient<FileDB>>,
    node_governance_address: Address,
    version_governance_address: Address,
}

impl HeliosGovernance {
    /// Create a new Helios governance client.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The Helios client fails to build or start
    /// - The provided contract addresses cannot be parsed
    pub async fn new(options: HeliosGovernanceOptions) -> Result<Self, Error> {
        let mut client = EthereumClientBuilder::new()
            .network(options.network)
            .data_dir(options.data_dir.clone())
            .consensus_rpc(&options.consensus_rpc)
            .execution_rpc(&options.execution_rpc)
            .load_external_fallback()
            .build()
            .map_err(Error::Helios)?;

        // Start the client and wait for sync before wrapping in Arc
        client.start().await.map_err(Error::Helios)?;
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
            .call(&tx, BlockTag::Latest)
            .await
            .map_err(Error::Helios)?;

        Ok(result.to_vec())
    }
}

#[async_trait]
impl Governance for HeliosGovernance {
    type Error = Error;

    async fn get_active_versions(&self) -> Result<Vec<Version>, Self::Error> {
        // Prepare the call data
        let selector = getActiveVersionsCall::SELECTOR;
        let result = self
            .call_contract_function(self.version_governance_address, &selector)
            .await?;

        let versions_result = getActiveVersionsCall::abi_decode_returns(&result, true)
            .map_err(|e| Error::ContractDataDecode(format!("Error decoding versions: {e}")))?;

        // Extract the version structs from the result
        let mut versions = Vec::new();

        // getActiveVersions returns VersionStruct[] memory, so versions_result._0 is the array
        for version_struct in &versions_result._0 {
            // Convert timestamp to SystemTime
            let activated_at_secs = version_struct.activatedAt.to::<u64>();
            let activated_at = UNIX_EPOCH + Duration::from_secs(activated_at_secs);

            if version_struct.active {
                versions.push(Version {
                    sequence: version_struct.sequence,
                    activated_at,
                    ne_pcr0: version_struct.nePcr0.clone(),
                    ne_pcr1: version_struct.nePcr1.clone(),
                    ne_pcr2: version_struct.nePcr2.clone(),
                });
            }
        }

        Ok(versions)
    }

    async fn get_topology(&self) -> Result<Vec<Node>, Self::Error> {
        // Prepare the call data
        let selector = getNodesCall::SELECTOR;
        let result = self
            .call_contract_function(self.node_governance_address, &selector)
            .await?;

        let nodes_result = getNodesCall::abi_decode_returns(&result, true)
            .map_err(|e| Error::ContractDataDecode(format!("Error decoding nodes: {e}")))?;

        // Extract the node structs from the result
        let mut nodes = Vec::new();

        // getNodes returns NodeStruct[] memory, so nodes_result._0 is the array
        for node_struct in &nodes_result._0 {
            let mut specializations = HashSet::new();

            // Parse specializations from bytes32 array
            for spec_bytes in &node_struct.specializations {
                // Convert bytes32 to string and check
                let spec_str = String::from_utf8_lossy(&spec_bytes[..]).to_string();
                if spec_str.starts_with("radix-mainnet") {
                    specializations.insert(NodeSpecialization::RadixMainnet);
                } else if spec_str.starts_with("radix-stokenet") {
                    specializations.insert(NodeSpecialization::RadixStokenet);
                }
            }

            nodes.push(Node {
                id: node_struct.id.clone(),
                region: node_struct.region.clone(),
                availability_zone: node_struct.availabilityZone.clone(),
                fqdn: node_struct.fqdn.clone(),
                public_key: node_struct.publicKey.clone(),
                specializations,
            });
        }

        Ok(nodes)
    }
}
