use url::Url;

/// RPC endpoints for the runtime.
#[derive(Clone, Debug)]
pub struct RpcEndpoints {
    /// Bitcoin mainnet RPC endpoint.
    pub bitcoin_mainnet: Url,

    /// Bitcoin testnet RPC endpoint.
    pub bitcoin_testnet: Url,

    /// Ethereum holesky RPC endpoint.
    pub ethereum_holesky: Url,

    /// Ethereum mainnet RPC endpoint.
    pub ethereum_mainnet: Url,

    /// Ethereum sepolia RPC endpoint.
    pub ethereum_sepolia: Url,

    /// Radix mainnet RPC endpoint.
    pub radix_mainnet: Url,

    /// Radix stokenet RPC endpoint.
    pub radix_stokenet: Url,
}

impl RpcEndpoints {
    /// Create a `RpcEndpoints` instance with all external endpoints (for testing purposes).
    pub fn external() -> Self {
        Self {
            bitcoin_mainnet: Url::parse("https://bitcoin-rpc.publicnode.com").unwrap(),
            bitcoin_testnet: Url::parse("https://bitcoin-testnet-rpc.publicnode.com").unwrap(),
            ethereum_holesky: Url::parse("https://ethereum-holesky-rpc.publicnode.com").unwrap(),
            ethereum_mainnet: Url::parse("https://ethereum-rpc.publicnode.com").unwrap(),
            ethereum_sepolia: Url::parse("https://ethereum-sepolia-rpc.publicnode.com").unwrap(),
            radix_mainnet: Url::parse("https://mainnet.radixdlt.com").unwrap(),
            radix_stokenet: Url::parse("https://stokenet.radixdlt.com").unwrap(),
        }
    }

    /// Convert the `RpcEndpoints` instance to a vector of URLs.
    pub fn into_vec(self) -> Vec<Url> {
        vec![
            self.bitcoin_mainnet,
            self.bitcoin_testnet,
            self.ethereum_holesky,
            self.ethereum_mainnet,
            self.ethereum_sepolia,
            self.radix_mainnet,
            self.radix_stokenet,
        ]
    }
}
