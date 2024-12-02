#![allow(clippy::result_large_err)]
mod bootstrap;
mod enclave;
mod error;
mod net;

use bootstrap::Bootstrap;
use enclave::Enclave;
use error::Result;

use std::sync::Arc;

use proven_vsock_rpc::{InitializeResponse, RpcCall, RpcServer, ShutdownResponse};
use tokio::sync::Mutex;
use tokio_vsock::{VsockAddr, VMADDR_CID_ANY};
use tracing::{error, info};

// TODO: Don't hardcode thread
#[tokio::main(worker_threads = 12)]
pub async fn main() -> Result<()> {
    let rpc_server = RpcServer::new(VsockAddr::new(VMADDR_CID_ANY, 1024));

    if let Err(e) = handle_initial_request(&rpc_server).await {
        error!("Failed to handle initial request: {:?}", e);
    }

    Ok(())
}

async fn handle_initial_request(rpc_server: &RpcServer) -> Result<()> {
    match rpc_server.accept().await {
        Ok(RpcCall::Initialize(args, ack)) => {
            let bootstrap = Bootstrap::new(args);

            match bootstrap.initialize().await {
                Ok(enclave) => {
                    info!("Enclave started successfully");
                    ack(InitializeResponse { success: true }).await.unwrap();
                    handle_requests_loop(rpc_server, enclave).await?;
                }
                Err(e) => {
                    error!("Failed to start enclave: {:?}", e);
                    ack(InitializeResponse { success: false }).await.unwrap();
                }
            }
        }
        Ok(_) => {
            error!("Unexpected initial request");
        }
        Err(e) => {
            error!("Failed to accept initial request: {:?}", e);
        }
    }
    Ok(())
}

async fn handle_requests_loop(rpc_server: &RpcServer, enclave: Enclave) -> Result<()> {
    let enclave = Arc::new(Mutex::new(enclave));

    loop {
        match rpc_server.accept().await {
            Ok(rpc) => match rpc {
                RpcCall::Initialize(_, ack) => {
                    error!("Already initialized");
                    ack(InitializeResponse { success: false }).await.unwrap();
                }
                RpcCall::AddPeer(args, ack) => {
                    let response = enclave.lock().await.add_peer(args).await;
                    ack(response).await.unwrap();
                }
                RpcCall::Shutdown(ack) => {
                    enclave.lock().await.shutdown().await;
                    ack(ShutdownResponse { success: true }).await.unwrap();
                    info!("Enclave shutdown successfully");
                    break;
                }
            },
            Err(e) => {
                error!("Failed to accept request: {:?}", e);
            }
        }
    }

    Ok(())
}
