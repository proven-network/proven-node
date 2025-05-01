use url::Url;

/// RPC endpoints for the runtime.
#[derive(Clone, Debug)]
pub struct RpcEndpoints {
    /// Bitcoin mainnet RPC endpoint.
    pub bitcoin_mainnet: Url,

    /// Bitcoin testnet RPC endpoint.
    pub bitcoin_testnet: Url,

    /// Bitcoin signet RPC endpoint.
    pub bitcoin_signet: Url,

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
            bitcoin_mainnet: Url::parse("https://mainnet.blockchain.info").unwrap(),
            bitcoin_testnet: Url::parse("https://testnet.blockchain.info").unwrap(),
            bitcoin_signet: Url::parse("https://signet.blockchain.info").unwrap(),
            ethereum_holesky: Url::parse("https://holesky.infura.io").unwrap(),
            ethereum_mainnet: Url::parse("https://mainnet.infura.io").unwrap(),
            ethereum_sepolia: Url::parse("https://sepolia.infura.io").unwrap(),
            radix_mainnet: Url::parse("https://mainnet.radixdlt.com").unwrap(),
            radix_stokenet: Url::parse("https://stokenet.radixdlt.com").unwrap(),
        }
    }

    /// Convert the `RpcEndpoints` instance to a vector of URLs.
    pub fn into_vec(self) -> Vec<Url> {
        vec![
            self.bitcoin_mainnet,
            self.bitcoin_testnet,
            self.bitcoin_signet,
            self.ethereum_holesky,
            self.ethereum_mainnet,
            self.ethereum_sepolia,
            self.radix_mainnet,
            self.radix_stokenet,
        ]
    }
}
