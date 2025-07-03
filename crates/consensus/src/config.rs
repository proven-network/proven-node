use openraft::Config;
use std::sync::Arc;
use tokio::time::Duration;

/// Configuration for the consensus manager
#[derive(Debug, Clone)]
pub struct ConsensusConfig {
    /// Timeout for consensus operations
    pub consensus_timeout: Duration,
    /// Whether to require all nodes for consensus (true) or just majority (false)
    pub require_all_nodes: bool,
    /// `OpenRaft` configuration
    pub raft_config: Arc<Config>,
    /// Directory path for persistent storage
    pub storage_dir: Option<String>,
    /// Timeout for cluster discovery before becoming initiator (None = use default 30s)
    pub cluster_discovery_timeout: Option<Duration>,
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        let raft_config = Config {
            heartbeat_interval: 500,    // 500ms
            election_timeout_min: 1500, // 1.5s
            election_timeout_max: 3000, // 3s
            ..Config::default()
        };

        Self {
            consensus_timeout: Duration::from_secs(30),
            require_all_nodes: false,
            raft_config: Arc::new(raft_config.validate().unwrap()),
            storage_dir: None, // Default to None, will use temporary directory
            cluster_discovery_timeout: None, // Default to None, will use 30s timeout
        }
    }
}
