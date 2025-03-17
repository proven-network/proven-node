#![allow(dead_code)]

use std::convert::Infallible;

use bytes::Bytes;
use proven_applications::{Application, ApplicationManager};
use proven_attestation_dev::DevAttestor;
use proven_bitcoin_core::BitcoinNode;
use proven_core::Core;
use proven_ethereum_lighthouse::LighthouseNode;
use proven_ethereum_reth::RethNode;
use proven_governance_mock::MockGovernance;
use proven_messaging_nats::stream::{NatsStream1, NatsStream2, NatsStream3};
use proven_nats_server::NatsServer;
use proven_postgres::Postgres;
use proven_radix_aggregator::RadixAggregator;
use proven_radix_gateway::RadixGateway;
use proven_radix_nft_verifier_gateway::GatewayRadixNftVerifier;
use proven_radix_node::RadixNode;
use proven_runtime::RuntimePoolManager;
use proven_sessions::{Session, SessionManager};
use proven_sql_streamed::{
    Request as SqlRequest, StreamedSqlStore1, StreamedSqlStore2, StreamedSqlStore3,
};
use proven_store_fs::{FsStore, FsStore1, FsStore2, FsStore3};
use proven_store_nats::{NatsStore, NatsStore1, NatsStore2, NatsStore3};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

pub type LocalNodeCore = Core<
    ApplicationManager<
        NatsStore<
            Application,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
        StreamedSqlStore1<
            NatsStream1<
                SqlRequest,
                ciborium::de::Error<std::io::Error>,
                ciborium::ser::Error<std::io::Error>,
            >,
            FsStore1<Bytes, Infallible, Infallible>,
        >,
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
    SessionManager<
        DevAttestor,
        NatsStore2,
        NatsStore1<
            Session,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
    >,
    DevAttestor,
    MockGovernance,
>;

/// A collection of all the services that can be running in the enclave.
pub struct Services {
    /// The Radix Mainnet node.
    pub radix_mainnet_node: Option<Arc<Mutex<RadixNode>>>,

    /// The Radix Stokenet node.
    pub radix_stokenet_node: Option<Arc<Mutex<RadixNode>>>,

    /// The Ethereum Reth node.
    pub ethereum_reth_node: Option<Arc<Mutex<RethNode>>>,

    /// The Ethereum Lighthouse node.
    pub ethereum_lighthouse_node: Option<Arc<Mutex<LighthouseNode>>>,

    /// The Bitcoin node (testnet or mainnet).
    pub bitcoin_node: Option<Arc<Mutex<BitcoinNode>>>,

    /// The Postgres database.
    pub postgres: Option<Arc<Mutex<Postgres>>>,

    /// The Radix Aggregator.
    pub radix_aggregator: Option<Arc<Mutex<RadixAggregator>>>,

    /// The Radix Gateway.
    pub radix_gateway: Option<Arc<Mutex<RadixGateway>>>,

    /// The NATS server.
    pub nats_server: Arc<Mutex<NatsServer>>,

    /// The Core.
    pub core: Arc<Mutex<LocalNodeCore>>,
}

pub struct LocalNode {
    attestor: DevAttestor,
    services: Services,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl LocalNode {
    pub const fn new(
        attestor: DevAttestor,
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
