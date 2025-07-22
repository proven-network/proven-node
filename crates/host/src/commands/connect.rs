use crate::ConnectArgs;
use crate::error::{Error, Result};
use crate::nitro::NitroCli;

use proven_logger_vsock::server::run_stdout_collector;
use tracing::info;

pub async fn connect(_args: ConnectArgs) -> Result<()> {
    if !NitroCli::is_enclave_running().await? {
        return Err(Error::NoRunningEnclave);
    }

    // Note: The new logger-vsock server listens on the host side
    // The enclave sends logs TO the host, so we bind to VMADDR_CID_ANY
    #[cfg(target_os = "linux")]
    let addr = tokio_vsock::VsockAddr::new(tokio_vsock::VMADDR_CID_ANY, 5555);

    #[cfg(not(target_os = "linux"))]
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 5555));

    info!("Starting log collector server on {:?}", addr);
    info!("Connected to enclave logs. Press Ctrl+C to exit.");

    // Use the convenient run_stdout_collector function
    run_stdout_collector(addr).await?;

    Ok(())
}
