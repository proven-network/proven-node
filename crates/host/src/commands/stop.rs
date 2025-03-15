use crate::StopArgs;
use crate::error::{Error, Result};
use crate::nitro::NitroCli;

use proven_vsock_rpc::RpcClient;
use tokio::time::{Duration, sleep};
use tokio_vsock::VsockAddr;
use tracing::info;

pub async fn stop(args: StopArgs) -> Result<()> {
    if !NitroCli::is_enclave_running().await? {
        info!("enclave is not running");
        return Ok(());
    }

    info!("attempting graceful shutdown");
    let _ = RpcClient::new(VsockAddr::new(args.enclave_cid, 1024))
        .shutdown()
        .await;

    let mut attempts = 0;
    while attempts < 60 {
        if !NitroCli::is_enclave_running().await? {
            info!("enclave shutdown successfully");
            return Ok(());
        }
        sleep(Duration::from_secs(1)).await;
        attempts += 1;
    }

    if args.force {
        info!("force shutting down enclave");
        NitroCli::terminate_all_enclaves().await?;
        info!("enclave terminated");

        Ok(())
    } else {
        info!("enclave is still running. use --force to terminate");

        Err(Error::EnclaveDidNotShutdown)
    }
}
