#![allow(dead_code)]

use std::convert::Infallible;

use bytes::Bytes;
use proven_applications::{ApplicationCommand, ApplicationEvent, ApplicationManager};
use proven_attestation_nsm::NsmAttestor;
use proven_core::Core;
use proven_dnscrypt_proxy::DnscryptProxy;
use proven_external_fs::ExternalFs;
use proven_governance_mock::MockGovernance;
use proven_http_letsencrypt::LetsEncryptHttpServer;
use proven_identity::{IdentityCommand, IdentityEvent, IdentityManager};
use proven_imds::IdentityDocument;
use proven_instance_details::Instance;
use proven_messaging_nats::stream::{NatsStream, NatsStream2, NatsStream3};
use proven_nats_server::NatsServer;
use proven_passkeys::{Passkey, PasskeyManager};
use proven_radix_nft_verifier_gateway::GatewayRadixNftVerifier;
use proven_runtime::RuntimePoolManager;
use proven_sessions::{Session, SessionManager};
use proven_sql_streamed::{Request as SqlRequest, StreamedSqlStore2, StreamedSqlStore3};
use proven_store_nats::{NatsStore, NatsStore1, NatsStore2, NatsStore3};
// use proven_nats_monitor::NatsMonitor;
use proven_postgres::Postgres;
use proven_radix_aggregator::RadixAggregator;
use proven_radix_gateway::RadixGateway;
use proven_radix_node::RadixNode;
use proven_store_s3::{S3Store, S3Store2, S3Store3};
use proven_vsock_proxy::Proxy;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

pub type EnclaveNodeCore = Core<
    ApplicationManager<
        NatsStream<
            ApplicationCommand,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
        NatsStream<
            ApplicationEvent,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
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
            S3Store2<Bytes, Infallible, Infallible>,
        >,
        StreamedSqlStore3<
            NatsStream3<
                SqlRequest,
                ciborium::de::Error<std::io::Error>,
                ciborium::ser::Error<std::io::Error>,
            >,
            S3Store3<Bytes, Infallible, Infallible>,
        >,
        StreamedSqlStore3<
            NatsStream3<
                SqlRequest,
                ciborium::de::Error<std::io::Error>,
                ciborium::ser::Error<std::io::Error>,
            >,
            S3Store3<Bytes, Infallible, Infallible>,
        >,
        S3Store<
            proven_runtime::StoredEntry,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
        GatewayRadixNftVerifier,
    >,
    IdentityManager<
        NatsStream<
            IdentityCommand,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
        NatsStream<
            IdentityEvent,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
    >,
    PasskeyManager<
        NatsStore<
            Passkey,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
    >,
    SessionManager<
        NsmAttestor,
        NatsStore1<
            Session,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
    >,
    NsmAttestor,
    MockGovernance,
    LetsEncryptHttpServer<S3Store<Bytes, Infallible, Infallible>>,
>;

pub struct Services {
    pub proxy: Arc<Mutex<Proxy>>,
    pub dnscrypt_proxy: Arc<Mutex<DnscryptProxy>>,
    pub radix_node_fs: Arc<Mutex<ExternalFs>>,
    pub radix_node: Arc<Mutex<RadixNode>>,
    pub postgres_fs: Arc<Mutex<ExternalFs>>,
    pub postgres: Arc<Mutex<Postgres>>,
    pub radix_aggregator: Arc<Mutex<RadixAggregator>>,
    pub radix_gateway: Arc<Mutex<RadixGateway>>,
    pub nats_server_fs: Arc<Mutex<ExternalFs>>,
    pub nats_server:
        Arc<Mutex<NatsServer<MockGovernance, NsmAttestor, S3Store<Bytes, Infallible, Infallible>>>>,
    pub core: Arc<Mutex<EnclaveNodeCore>>,
}

pub struct EnclaveNode {
    attestor: NsmAttestor,
    imds_identity: IdentityDocument,
    instance_details: Instance,
    services: Services,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl EnclaveNode {
    pub const fn new(
        attestor: NsmAttestor,
        imds_identity: IdentityDocument,
        instance_details: Instance,
        services: Services,
        shutdown_token: CancellationToken,
        task_tracker: TaskTracker,
    ) -> Self {
        Self {
            attestor,
            imds_identity,
            instance_details,
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
