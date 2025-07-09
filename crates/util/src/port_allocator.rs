use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    net::{SocketAddr, TcpListener},
    sync::{LazyLock, Mutex},
    time::SystemTime,
};

/// Global port allocator starting from a random port (to avoid conflicts with other services)
static NEXT_PORT: LazyLock<Mutex<u16>> = LazyLock::new(|| {
    let mut hasher = DefaultHasher::new();
    SystemTime::now().hash(&mut hasher);
    // Generate random port in range 15000-25000
    let random_offset = (hasher.finish() % 10000) as u16;
    let starting_port = 15000 + random_offset;
    Mutex::new(starting_port)
});

/// Allocate the next available port, starting from a random port in range 15000-25000
pub fn allocate_port() -> u16 {
    let mut port_guard = NEXT_PORT.lock().unwrap();

    // Try up to 10000 ports from the current position
    for _ in 0..10000 {
        let port = *port_guard;
        *port_guard += 1;

        // Check if port is actually available on the system
        if is_port_available(port) {
            return port;
        }
    }

    panic!(
        "No available ports found after trying 10000 ports starting from {}",
        *port_guard - 10000
    )
}

/// Check if a port is available by attempting to bind to it
pub fn is_port_available(port: u16) -> bool {
    TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port))).is_ok()
}

/// Allocate a socket address with an available port
pub fn allocate_socket_addr() -> SocketAddr {
    let port = allocate_port();
    SocketAddr::from(([127, 0, 0, 1], port))
}
