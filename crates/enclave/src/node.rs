#![allow(dead_code)]
#![allow(clippy::type_complexity)]

use std::convert::Infallible;

use bytes::Bytes;
use proven_attestation_nsm::NsmAttestor;
use proven_core::Core;
use proven_dnscrypt_proxy::DnscryptProxy;
use proven_external_fs::ExternalFs;
use proven_http_letsencrypt::LetsEncryptHttpServer;
use proven_imds::IdentityDocument;
use proven_instance_details::Instance;
use proven_network::Node;
use proven_postgres::Postgres;
use proven_radix_aggregator::RadixAggregator;
use proven_radix_gateway::RadixGateway;
use proven_radix_node::RadixNode;
use proven_store_s3::S3Store;
use proven_topology_mock::MockTopologyAdaptor;
use proven_vsock_proxy::Proxy;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::info;

pub type EnclaveNodeCore = Core<
    NsmAttestor,
    MockTopologyAdaptor,
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
