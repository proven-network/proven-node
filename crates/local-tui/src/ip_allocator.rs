use std::{
    net::{SocketAddr, TcpListener},
    sync::{LazyLock, Mutex},
};

use anyhow::Result;

/// Global port allocator starting from 3000
static NEXT_PORT: LazyLock<Mutex<u16>> = LazyLock::new(|| Mutex::new(3000));

/// Allocate the next available port, starting from 3000
pub fn allocate_port() -> Result<u16> {
    let mut port_guard = NEXT_PORT.lock().unwrap();

    // Try up to 10000 ports from the current position
    for _ in 0..10000 {
        let port = *port_guard;
        *port_guard += 1;

        // Check if port is actually available on the system
        if is_port_available(port) {
            return Ok(port);
        }
    }

    anyhow::bail!(
        "No available ports found after trying 10000 ports from {}",
        *port_guard - 10000
    )
}

/// Check if a port is available by attempting to bind to it
pub fn is_port_available(port: u16) -> bool {
    TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port))).is_ok()
}
