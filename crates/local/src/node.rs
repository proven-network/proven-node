#![allow(dead_code)]
#![allow(clippy::type_complexity)]

use std::convert::Infallible;

use bytes::Bytes;
use proven_applications::ApplicationManager;
use proven_attestation_mock::MockAttestor;
use proven_bitcoin_core::BitcoinNode;
use proven_core::Core;
use proven_ethereum_lighthouse::LighthouseNode;
use proven_ethereum_reth::RethNode;
use proven_governance_mock::MockGovernance;
use proven_http_insecure::InsecureHttpServer;
use proven_identity::IdentityManager;
use proven_locks_nats::NatsLockManager;
use proven_messaging_nats::stream::{NatsStream, NatsStream2, NatsStream3};
use proven_nats_server::NatsServer;
use proven_passkeys::{Passkey, PasskeyManager};
use proven_postgres::Postgres;
use proven_radix_aggregator::RadixAggregator;
use proven_radix_gateway::RadixGateway;
use proven_radix_nft_verifier_gateway::GatewayRadixNftVerifier;
use proven_radix_node::RadixNode;
use proven_runtime::RuntimePoolManager;
use proven_sessions::{Session, SessionManager};
use proven_sql_streamed::{Request as SqlRequest, StreamedSqlStore2, StreamedSqlStore3};
use proven_store_fs::{FsStore, FsStore2, FsStore3};
use proven_store_nats::{NatsStore, NatsStore1, NatsStore2, NatsStore3};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

pub type LocalNodeCore = Core<
    ApplicationManager<
        NatsStream<
            proven_applications::Command,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
        NatsStream<
            proven_applications::Event,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
        NatsLockManager,
    >,
    RuntimePoolManager<
        NatsStore2,
        NatsStore3,
        NatsStore3,
        StreamedSqlStore2<
            NatsStream2<
                SqlRequest,
                ciborium::de::Error<std::io::Error>,
                ciborium::ser::Error<std::io::Error>,
            >,
            FsStore2<Bytes, Infallible, Infallible>,
        >,
        StreamedSqlStore3<
            NatsStream3<
                SqlRequest,
                ciborium::de::Error<std::io::Error>,
                ciborium::ser::Error<std::io::Error>,
            >,
            FsStore3<Bytes, Infallible, Infallible>,
        >,
        StreamedSqlStore3<
            NatsStream3<
                SqlRequest,
                ciborium::de::Error<std::io::Error>,
                ciborium::ser::Error<std::io::Error>,
            >,
            FsStore3<Bytes, Infallible, Infallible>,
        >,
        FsStore<
            proven_runtime::StoredEntry,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
        GatewayRadixNftVerifier,
    >,
    IdentityManager<
        NatsStream<
            proven_identity::Command,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
        NatsStream<
            proven_identity::Event,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
        NatsLockManager,
    >,
    PasskeyManager<
        NatsStore<
            Passkey,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
    >,
    SessionManager<
        MockAttestor,
        NatsStore1<
            Session,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
    >,
    MockAttestor,
    MockGovernance,
    InsecureHttpServer,
>;

/// A collection of all the services that can be running in the enclave.
pub struct Services {
    /// The Bitcoin node (mainnet).
    pub bitcoin_mainnet_node: Option<Arc<Mutex<BitcoinNode>>>,

    /// The Bitcoin node (testnet).
    pub bitcoin_testnet_node: Option<Arc<Mutex<BitcoinNode>>>,

    /// The Core.
    pub core: Arc<Mutex<LocalNodeCore>>,

    /// The Ethereum Holesky Reth node (holesky).
    pub ethereum_holesky_reth_node: Option<Arc<Mutex<RethNode>>>,

    /// The Ethereum Holesky Lighthouse node (holesky).
    pub ethereum_holesky_lighthouse_node: Option<Arc<Mutex<LighthouseNode>>>,

    /// The Ethereum Mainnet Reth node (mainnet).
    pub ethereum_mainnet_reth_node: Option<Arc<Mutex<RethNode>>>,

    /// The Ethereum Mainnet Lighthouse node (mainnet).
    pub ethereum_mainnet_lighthouse_node: Option<Arc<Mutex<LighthouseNode>>>,

    /// The Ethereum Sepolia Reth node (sepolia).
    pub ethereum_sepolia_reth_node: Option<Arc<Mutex<RethNode>>>,

    /// The Ethereum Sepolia Lighthouse node (sepolia).
    pub ethereum_sepolia_lighthouse_node: Option<Arc<Mutex<LighthouseNode>>>,

    /// The NATS server.
    pub nats_server: Arc<
        Mutex<NatsServer<MockGovernance, MockAttestor, FsStore<Bytes, Infallible, Infallible>>>,
    >,

    /// The Postgres database.
    pub postgres: Option<Arc<Mutex<Postgres>>>,

    /// The Radix Mainnet node (mainnet).
    pub radix_mainnet_node: Option<Arc<Mutex<RadixNode>>>,

    /// The Radix Aggregator (mainnet).
    pub radix_mainnet_aggregator: Option<Arc<Mutex<RadixAggregator>>>,

    /// The Radix Gateway (mainnet).
    pub radix_mainnet_gateway: Option<Arc<Mutex<RadixGateway>>>,

    /// The Radix Stokenet node (stokenet).
    pub radix_stokenet_node: Option<Arc<Mutex<RadixNode>>>,

    /// The Radix Aggregator (stokenet).
    pub radix_stokenet_aggregator: Option<Arc<Mutex<RadixAggregator>>>,

    /// The Radix Gateway (stokenet).
    pub radix_stokenet_gateway: Option<Arc<Mutex<RadixGateway>>>,
}

pub struct LocalNode {
    attestor: MockAttestor,
    services: Services,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl LocalNode {
    pub const fn new(
        attestor: MockAttestor,
        services: Services,
        shutdown_token: CancellationToken,
        task_tracker: TaskTracker,
    ) -> Self {
        Self {
            attestor,
            services,
            shutdown_token,
            task_tracker,
        }
    }

    pub async fn shutdown(&self) {
        info!("enclave shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("enclave shutdown");
    }
}
