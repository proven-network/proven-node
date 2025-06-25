//! Main entrypoint for enclave images. Bootstraps all other components before
//! handing off to core.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::redundant_pub_crate)]
#![allow(clippy::result_large_err)]
#![allow(clippy::large_futures)]

mod bootstrap;
mod error;
mod fdlimit;
mod net;
mod node;
mod speedtest;

use bootstrap::Bootstrap;
use error::{Error, Result};
use node::EnclaveNode;
use proven_vsock_tracing::enclave::VsockTracingProducer;

use std::sync::Arc;

use proven_vsock_rpc::{InitializeResponse, RpcCall, RpcServer, ShutdownResponse};
use tokio::sync::Mutex;
use tokio_vsock::{VMADDR_CID_ANY, VsockAddr};
use tracing::{error, info};
use tracing_panic::panic_hook;

// TODO: Don't hardcode threads
#[tokio::main(worker_threads = 12)]
async fn main() -> Result<()> {
    // Configure logging
    std::panic::set_hook(Box::new(panic_hook));
    VsockTracingProducer::start()?;

    fdlimit::raise_fdlimit();

    let rpc_server = RpcServer::new(VsockAddr::new(VMADDR_CID_ANY, 1024));

    if let Err(e) = handle_initial_request(&rpc_server).await {
        error!("Failed to handle initial request: {:?}", e);
    }

    Ok(())
}

async fn handle_initial_request(rpc_server: &RpcServer) -> Result<()> {
    match rpc_server.accept().await {
        Ok(RpcCall::Initialize(args, ack)) => {
            let Ok(bootstrap) = Bootstrap::new(args) else {
                ack(InitializeResponse { success: false }).await?;
                return Ok(());
            };

            match bootstrap.initialize().await {
                Ok(enclave) => {
                    info!("Enclave started successfully");

                    ack(InitializeResponse { success: true }).await?;

                    handle_requests_loop(rpc_server, enclave).await?;
                }

                Err(e) => {
                    error!("Failed to start enclave: {:?}", e);
                    ack(InitializeResponse { success: false }).await?;
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

async fn handle_requests_loop(rpc_server: &RpcServer, enclave: EnclaveNode) -> Result<()> {
    let enclave = Arc::new(Mutex::new(enclave));

    loop {
        match rpc_server.accept().await {
            Ok(rpc) => match rpc {
                RpcCall::Initialize(_, ack) => {
                    error!("Already initialized");
                    ack(InitializeResponse { success: false }).await?;
                }

                RpcCall::Shutdown(ack) => {
                    enclave.lock().await.shutdown().await;

                    ack(ShutdownResponse { success: true }).await?;

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
