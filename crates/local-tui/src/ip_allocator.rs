use std::{
    net::{SocketAddr, TcpListener},
    sync::{LazyLock, Mutex},
};

use anyhow::Result;

/// Global port allocator starting from 3000
static NEXT_PORT: LazyLock<Mutex<u16>> = LazyLock::new(|| Mutex::new(3000));

/// Allocate the next available port, starting from 3000
///
/// # Errors
///
/// Returns an error if no available ports can be found after checking 10000 ports
///
/// # Panics
///
/// Panics if the mutex is poisoned
pub fn allocate_port() -> Result<u16> {
    // Try up to 10000 ports from the current position
    for _ in 0..10000 {
        let port = {
            let mut port_guard = NEXT_PORT.lock().unwrap();
            let port = *port_guard;
            *port_guard += 1;
            port
        };

        // Check if port is actually available on the system
        if is_port_available(port) {
            return Ok(port);
        }
    }

    let start_port = NEXT_PORT.lock().unwrap().saturating_sub(10000);
    anyhow::bail!(
        "No available ports found after trying 10000 ports from {}",
        start_port
    )
}

/// Check if a port is available by attempting to bind to it
#[must_use]
pub fn is_port_available(port: u16) -> bool {
    TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port))).is_ok()
}
