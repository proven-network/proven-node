use crate::error::Result;

use std::net::SocketAddrV4;

use proven_attestation_nsm::NsmAttestor;
use proven_core::Core;
use proven_nats_server::NatsServer;
use proven_sessions::SessionManager;
use proven_store_nats::NatsStore;
use proven_vsock_rpc::{AddPeerArgs, Command};
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

    pub async fn handle_command(&self, command: Command) {
        match command {
            Command::AddPeer(args) => {
                if let Err(e) = self.add_peer(args).await {
                    error!("add_peer failed: {:?}", e);
                }
            }
            _ => {
                error!("unknown command");
            }
        }
    }

    async fn add_peer(&self, args: AddPeerArgs) -> Result<()> {
        self.nats_server
            .add_peer(SocketAddrV4::new(args.peer_ip, args.peer_port))
            .await?;

        Ok(())
    }

    pub async fn shutdown(&self) {
        info!("enclave shutting down...");

        self.core.shutdown().await;
        self.nats_server.shutdown().await;

        info!("enclave shutdown");
    }
}
