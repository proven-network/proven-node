#![allow(dead_code)]

use std::{convert::Infallible, net::SocketAddrV4};

use bytes::Bytes;
use proven_applications::{Application, ApplicationManager};
use proven_attestation_nsm::NsmAttestor;
use proven_core::Core;
use proven_dnscrypt_proxy::DnscryptProxy;
use proven_external_fs::ExternalFs;
use proven_imds::IdentityDocument;
use proven_instance_details::Instance;
use proven_messaging_nats::stream::{NatsStream1, NatsStream2, NatsStream3};
use proven_nats_server::NatsServer;
use proven_radix_nft_verifier_gateway::GatewayRadixNftVerifier;
use proven_runtime::RuntimePoolManager;
use proven_sessions::{Session, SessionManager};
use proven_sql_streamed::{
    Request as SqlRequest, StreamedSqlStore1, StreamedSqlStore2, StreamedSqlStore3,
};
use proven_store_nats::{NatsStore, NatsStore1, NatsStore2, NatsStore3};
// use proven_nats_monitor::NatsMonitor;
use proven_postgres::Postgres;
use proven_radix_aggregator::RadixAggregator;
use proven_radix_gateway::RadixGateway;
use proven_radix_node::RadixNode;
use proven_store_s3::{S3Store, S3Store1, S3Store2, S3Store3};
use proven_vsock_proxy::Proxy;
use proven_vsock_rpc::{AddPeerRequest, AddPeerResponse};
use radix_common::network::NetworkDefinition;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

pub type EnclaveCore = Core<
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
            S3Store1<Bytes, Infallible, Infallible>,
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
    SessionManager<
        NsmAttestor,
        NatsStore2,
        NatsStore1<
            Session,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
    >,
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
    pub nats_server: Arc<Mutex<NatsServer>>,
    pub core: Arc<Mutex<EnclaveCore>>,
}

pub struct Enclave {
    nsm: NsmAttestor,
    network_definition: NetworkDefinition,
    imds_identity: IdentityDocument,
    instance_details: Instance,
    services: Services,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl Enclave {
    pub const fn new(
        nsm: NsmAttestor,
        network_definition: NetworkDefinition,
        imds_identity: IdentityDocument,
        instance_details: Instance,
        services: Services,
        shutdown_token: CancellationToken,
        task_tracker: TaskTracker,
    ) -> Self {
        Self {
            nsm,
            network_definition,
            imds_identity,
            instance_details,
            services,
            shutdown_token,
            task_tracker,
        }
    }

    pub async fn add_peer(&self, args: AddPeerRequest) -> AddPeerResponse {
        let result = self
            .services
            .nats_server
            .lock()
            .await
            .add_peer(SocketAddrV4::new(args.peer_ip, args.peer_port));

        match result {
            Ok(()) => AddPeerResponse { success: true },
            Err(e) => {
                error!("failed to add peer: {:?}", e);

                AddPeerResponse { success: false }
            }
        }
    }

    pub async fn shutdown(&self) {
        info!("enclave shutting down...");

        self.shutdown_token.cancel();
        self.task_tracker.wait().await;

        info!("enclave shutdown");
    }
}
