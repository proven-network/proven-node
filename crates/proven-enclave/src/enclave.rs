use std::net::SocketAddrV4;

use proven_attestation_nsm::NsmAttestor;
use proven_core::Core;
use proven_nats_server::NatsServer;
use proven_sessions::SessionManager;
use proven_store_nats::NatsStore;
use proven_vsock_rpc::{AddPeerRequest, AddPeerResponse};
use tracing::{error, info};

pub struct Enclave {
    core: Core<SessionManager<NsmAttestor, NatsStore, NatsStore>>,
    nats_server: NatsServer,
}

impl Enclave {
    pub fn new(
        core: Core<SessionManager<NsmAttestor, NatsStore, NatsStore>>,
        nats_server: NatsServer,
    ) -> Self {
        Self { core, nats_server }
    }

    pub async fn add_peer(&self, args: AddPeerRequest) -> AddPeerResponse {
        match self
            .nats_server
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

        self.core.shutdown().await;
        self.nats_server.shutdown().await;

        info!("enclave shutdown");
    }
}
