//! Connection pooling and management.

use crate::error::{ConnectionError, Error, Result};
use crate::protocol::{Frame, FrameCodec, FrameType, ResponseEnvelope, codec};
use dashmap::DashMap;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, oneshot};
use tokio::time::{interval, timeout};
use tokio_util::codec::Framed;
use tokio_vsock::{VsockAddr, VsockStream};
use tracing::{debug, error, warn};
use uuid::Uuid;

/// Type alias for response sender channel.
pub type ResponseSender = oneshot::Sender<Result<ResponseEnvelope>>;

/// Configuration for connection pooling.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum connections in pool.
    pub max_connections: usize,
    /// Minimum idle connections to maintain.
    pub min_idle: usize,
    /// Connection timeout.
    pub connect_timeout: Duration,
    /// Idle timeout before pruning.
    pub idle_timeout: Duration,
    /// Health check interval.
    pub health_check_interval: Duration,
    /// Maximum in-flight requests per connection.
    pub max_in_flight: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            min_idle: 2,
            connect_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(300),
            health_check_interval: Duration::from_secs(30),
            max_in_flight: 100,
        }
    }
}

/// Health status of a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionHealth {
    /// Connection is healthy.
    Healthy,
    /// Connection health is unknown.
    Unknown,
    /// Connection is unhealthy.
    Unhealthy,
}

/// A pooled connection wrapper.
pub struct PooledConnection {
    id: u64,
    sink: Arc<tokio::sync::Mutex<SplitSink<Framed<VsockStream, FrameCodec>, Frame>>>,
    last_used: Arc<RwLock<Instant>>,
    in_flight: Arc<RwLock<usize>>,
    health: Arc<RwLock<ConnectionHealth>>,
}

impl PooledConnection {
    /// Get the connection ID.
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// Check if the connection is healthy.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        *self.health.read() == ConnectionHealth::Healthy
    }

    /// Get the number of in-flight requests.
    #[must_use]
    pub fn in_flight_count(&self) -> usize {
        *self.in_flight.read()
    }

    /// Check if the connection has capacity for more requests.
    #[must_use]
    pub fn has_capacity(&self, max_in_flight: usize) -> bool {
        self.in_flight_count() < max_in_flight
    }

    /// Send a frame through this connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the frame cannot be sent.
    pub async fn send_frame(&self, frame: Frame) -> Result<()> {
        // Increment in-flight counter
        {
            let mut in_flight = self.in_flight.write();
            *in_flight += 1;
        }

        // Send frame
        let result = {
            let mut sink = self.sink.lock().await;
            sink.send(frame).await.map_err(Error::Io)
        };

        // Update last used time
        *self.last_used.write() = Instant::now();

        // Decrement in-flight counter on completion
        {
            let mut in_flight = self.in_flight.write();
            *in_flight = in_flight.saturating_sub(1);
        }

        result
    }

    /// Mark the connection as unhealthy.
    pub fn mark_unhealthy(&self) {
        *self.health.write() = ConnectionHealth::Unhealthy;
    }
}

/// Connection pool for managing multiple connections.
pub struct ConnectionPool {
    addr: VsockAddr,
    config: PoolConfig,
    connections: Arc<RwLock<Vec<Arc<PooledConnection>>>>,
    semaphore: Arc<Semaphore>,
    next_id: Arc<RwLock<u64>>,
    shutdown: Arc<RwLock<bool>>,
    pending_requests: Arc<DashMap<Uuid, ResponseSender>>,
}

impl ConnectionPool {
    /// Create a new connection pool.
    #[must_use]
    pub fn new(
        addr: VsockAddr,
        config: PoolConfig,
        pending_requests: Arc<DashMap<Uuid, ResponseSender>>,
    ) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_connections));

        Self {
            addr,
            config,
            connections: Arc::new(RwLock::new(Vec::new())),
            semaphore,
            next_id: Arc::new(RwLock::new(0)),
            shutdown: Arc::new(RwLock::new(false)),
            pending_requests,
        }
    }

    /// Start the connection pool background tasks.
    pub fn start(&self) {
        // Start health check task
        let pool = self.clone();
        tokio::spawn(async move {
            pool.health_check_loop().await;
        });

        // Start idle connection maintenance
        let pool = self.clone();
        tokio::spawn(async move {
            pool.maintain_min_idle_loop().await;
        });
    }

    /// Get a connection from the pool.
    ///
    /// # Errors
    ///
    /// Returns an error if the pool is exhausted or connection fails.
    pub async fn get(&self) -> Result<Arc<PooledConnection>> {
        // Check if we're shutting down
        if *self.shutdown.read() {
            return Err(ConnectionError::ShuttingDown.into());
        }

        // Try to find a healthy connection with capacity
        {
            let connections = self.connections.read();
            for conn in connections.iter() {
                if conn.is_healthy() && conn.has_capacity(self.config.max_in_flight) {
                    return Ok(Arc::clone(conn));
                }
            }
        }

        // No suitable connection found, try to create a new one
        let _permit = self
            .semaphore
            .try_acquire()
            .map_err(|_| Error::PoolExhausted("Maximum connections reached".to_string()))?;

        let conn = self.create_connection().await?;

        // Add to pool
        {
            let mut connections = self.connections.write();
            connections.push(Arc::clone(&conn));
        }

        // Release permit when connection is added
        // Permit is automatically dropped here

        Ok(conn)
    }

    /// Create a new connection.
    async fn create_connection(&self) -> Result<Arc<PooledConnection>> {
        let stream = timeout(self.config.connect_timeout, VsockStream::connect(self.addr))
            .await
            .map_err(|_| Error::Timeout(self.config.connect_timeout))?
            .map_err(|e| ConnectionError::ConnectFailed {
                addr: self.addr,
                source: e,
            })?;

        let framed = Framed::new(stream, FrameCodec::new());
        let (sink, stream) = framed.split();

        let id = {
            let mut next_id = self.next_id.write();
            let id = *next_id;
            *next_id += 1;
            id
        };

        let (_shutdown_tx, shutdown_rx) = oneshot::channel();

        let conn = Arc::new(PooledConnection {
            id,
            sink: Arc::new(tokio::sync::Mutex::new(sink)),
            last_used: Arc::new(RwLock::new(Instant::now())),
            in_flight: Arc::new(RwLock::new(0)),
            health: Arc::new(RwLock::new(ConnectionHealth::Healthy)),
        });

        // Start stream handler
        let conn_clone = Arc::clone(&conn);
        let pending_requests = Arc::clone(&self.pending_requests);
        tokio::spawn(async move {
            Self::handle_stream(conn_clone, stream, shutdown_rx, pending_requests).await;
        });

        debug!("Created new connection {} to {:?}", id, self.addr);

        Ok(conn)
    }

    /// Handle incoming frames from a connection.
    #[allow(clippy::cognitive_complexity)]
    async fn handle_stream(
        _conn: Arc<PooledConnection>,
        mut stream: SplitStream<Framed<VsockStream, FrameCodec>>,
        mut shutdown_rx: oneshot::Receiver<()>,
        pending_requests: Arc<DashMap<Uuid, ResponseSender>>,
    ) {
        loop {
            tokio::select! {
                frame = stream.next() => {
                    match frame {
                        Some(Ok(frame)) => {
                            match frame.frame_type {
                                FrameType::Response => {
                                    // Decode response envelope
                                    match codec::decode::<ResponseEnvelope>(&frame.payload) {
                                        Ok(response) => {
                                            // Find and notify waiting request
                                            if let Some((_, sender)) = pending_requests.remove(&response.request_id) {
                                                let _ = sender.send(Ok(response));
                                            } else {
                                                warn!("Received response for unknown request: {}", response.request_id);
                                            }
                                        }
                                        Err(e) => {
                                            error!("Failed to decode response: {}", e);
                                        }
                                    }
                                }
                                _ => {
                                    debug!("Received unexpected frame type: {:?}", frame.frame_type);
                                }
                            }
                        }
                        Some(Err(e)) => {
                            error!("Stream error: {}", e);
                            break;
                        }
                        None => {
                            debug!("Stream closed");
                            break;
                        }
                    }
                }
                _ = &mut shutdown_rx => {
                    debug!("Connection shutdown requested");
                    break;
                }
            }
        }
    }

    /// Health check loop.
    async fn health_check_loop(&self) {
        let mut ticker = interval(self.config.health_check_interval);

        loop {
            ticker.tick().await;

            if *self.shutdown.read() {
                break;
            }

            let connections = self.connections.read().clone();
            for conn in connections {
                if conn.is_healthy() {
                    // Send ping
                    let ping = Frame::new(FrameType::Heartbeat, vec![].into());

                    if let Err(e) = conn.send_frame(ping).await {
                        warn!("Health check failed for connection {}: {}", conn.id(), e);
                        conn.mark_unhealthy();
                    }
                }
            }

            // Remove unhealthy connections
            self.prune_unhealthy_connections();
        }
    }

    /// Maintain minimum idle connections.
    async fn maintain_min_idle_loop(&self) {
        let mut ticker = interval(Duration::from_secs(10));

        loop {
            ticker.tick().await;

            if *self.shutdown.read() {
                break;
            }

            let current_count = self.connections.read().len();
            if current_count < self.config.min_idle {
                let needed = self.config.min_idle - current_count;
                for _ in 0..needed {
                    match self.create_connection().await {
                        Ok(conn) => {
                            let mut connections = self.connections.write();
                            connections.push(conn);
                        }
                        Err(e) => {
                            warn!("Failed to create idle connection: {}", e);
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Remove unhealthy connections from the pool.
    fn prune_unhealthy_connections(&self) {
        let mut connections = self.connections.write();
        connections.retain(|conn| conn.is_healthy());
    }

    /// Shutdown the connection pool.
    pub fn shutdown(&self) {
        *self.shutdown.write() = true;

        // Clear connections
        // Note: Connections will be cleaned up when dropped
        self.connections.write().clear();
    }
}

impl Clone for ConnectionPool {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr,
            config: self.config.clone(),
            connections: Arc::clone(&self.connections),
            semaphore: Arc::clone(&self.semaphore),
            next_id: Arc::clone(&self.next_id),
            shutdown: Arc::clone(&self.shutdown),
            pending_requests: Arc::clone(&self.pending_requests),
        }
    }
}
