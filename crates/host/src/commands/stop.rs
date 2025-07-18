use crate::StopArgs;
use crate::error::{Error, Result};
use crate::nitro::NitroCli;

use proven_vsock_cac::CacClient;
use tokio::time::{Duration, sleep};
use tracing::info;

#[allow(clippy::cognitive_complexity)]
pub async fn stop(args: StopArgs) -> Result<()> {
    if !NitroCli::is_enclave_running().await? {
        info!("enclave is not running");
        return Ok(());
    }

    info!("attempting graceful shutdown");
    if let Ok(client) = CacClient::new(
        #[cfg(target_os = "linux")]
        tokio_vsock::VsockAddr::new(args.enclave_cid, 1024),
        #[cfg(not(target_os = "linux"))]
        std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 1024)),
    ) {
        let _ = client.shutdown().await;
    }

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
