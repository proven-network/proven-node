use crate::{Error, Result};
use systemctl::SystemCtl;

/// Restarts the nitro-enclaves-allocator service.
///
/// # Errors
///
/// Returns an `Error::Io` if the service fails to restart.
pub fn restart_allocator_service() -> Result<()> {
    SystemCtl::default()
        .restart("nitro-enclaves-allocator.service")
        .map_err(|e| Error::Io("failed to restart nitro-enclaves-allocator.service", e))?;

    Ok(())
}
