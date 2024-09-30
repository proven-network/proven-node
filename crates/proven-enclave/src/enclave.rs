#![allow(dead_code)]

use std::net::SocketAddrV4;

use proven_attestation_nsm::NsmAttestor;
use proven_core::Core;
use proven_dnscrypt_proxy::DnscryptProxy;
use proven_external_fs::ExternalFs;
use proven_imds::IdentityDocument;
use proven_instance_details::Instance;
use proven_nats_server::NatsServer;
use proven_sessions::SessionManager;
use proven_store_nats::NatsStore;
// use proven_nats_monitor::NatsMonitor;
use proven_postgres::Postgres;
use proven_radix_aggregator::RadixAggregator;
use proven_radix_gateway::RadixGateway;
use proven_radix_node::RadixNode;
use proven_vsock_proxy::Proxy;
use proven_vsock_rpc::{AddPeerRequest, AddPeerResponse};
use radix_common::network::NetworkDefinition;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

pub struct EnclaveServices {
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
    pub core: Arc<Mutex<Core<SessionManager<NsmAttestor, NatsStore, NatsStore>>>>,
}

pub struct Enclave {
    nsm: NsmAttestor,
    network_definition: NetworkDefinition,
    imds_identity: IdentityDocument,
    instance_details: Instance,
    enclave_services: EnclaveServices,
    shutdown_token: CancellationToken,
    task_tracker: TaskTracker,
}

impl Enclave {
    pub fn new(
        nsm: NsmAttestor,
        network_definition: NetworkDefinition,
        imds_identity: IdentityDocument,
        instance_details: Instance,
        enclave_services: EnclaveServices,
        shutdown_token: CancellationToken,
        task_tracker: TaskTracker,
    ) -> Self {
        Self {
            nsm,
            network_definition,
            imds_identity,
            instance_details,
            enclave_services,
            shutdown_token,
            task_tracker,
        }
    }

    pub async fn add_peer(&self, args: AddPeerRequest) -> AddPeerResponse {
        match self
            .enclave_services
            .nats_server
            .lock()
            .await
            .add_peer(SocketAddrV4::new(args.peer_ip, args.peer_port))
            .await
        {
            Ok(_) => AddPeerResponse { success: true },
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
