//! Simplified connection pool using the unified Connection state machine

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;
use ed25519_dalek::SigningKey;
use flume;
use proven_attestation::Attestor;
use proven_topology::{Node, NodeId, TopologyAdaptor};
use proven_transport::{Connection as TransportConnection, Transport};
use tokio::sync::{Mutex, oneshot};
use tracing::{debug, warn};

use crate::attestation::AttestationVerifier;
use crate::connection::{Connection, VerifiedMessage};
use crate::connection_verifier::ConnectionVerifier;
use crate::cose::CoseHandler;
use crate::error::{NetworkError, NetworkResult};

/// Type alias for connections map
type ConnectionsMap<G, A> = Arc<DashMap<NodeId, Vec<Arc<Connection<G, A>>>>>;

/// Type alias for pending connections map
type PendingConnectionsMap<G, A> =
    Arc<DashMap<NodeId, Arc<Mutex<Option<oneshot::Receiver<Arc<Connection<G, A>>>>>>>>;

/// Type alias for connections by ID map
type ConnectionsByIdMap<G, A> = Arc<DashMap<String, Arc<Connection<G, A>>>>;

/// Configuration for connection pool
#[derive(Debug, Clone)]
pub struct ConnectionPoolConfig {
    /// Maximum connections per peer
    pub max_connections_per_peer: usize,
    /// Connection idle timeout
    pub idle_timeout: std::time::Duration,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_connections_per_peer: 3,
            idle_timeout: std::time::Duration::from_secs(300),
        }
    }
}

/// Simplified connection pool using unified connections
pub struct ConnectionPool<T, G, A>
where
    T: Transport,
    G: TopologyAdaptor,
    A: Attestor,
{
    /// Transport for creating connections
    pub transport: Arc<T>,
    /// Our node ID
    our_node_id: NodeId,
    /// COSE handler for signing/verification
    cose_handler: Arc<CoseHandler>,
    /// Connection verifier
    connection_verifier: Arc<ConnectionVerifier<G, A>>,
    /// Active connections by peer ID
    connections: ConnectionsMap<G, A>,
    /// Connections by ID (for incoming connections before we know peer)
    connections_by_id: ConnectionsByIdMap<G, A>,
    /// Pending outgoing connections (to prevent concurrent attempts)
    pending_connections: PendingConnectionsMap<G, A>,
    /// Configuration
    _config: ConnectionPoolConfig,
    /// Channel for incoming verified messages
    incoming_tx: flume::Sender<VerifiedMessage>,
    /// Next connection ID
    next_conn_id: AtomicUsize,
}

impl<T, G, A> ConnectionPool<T, G, A>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static + std::fmt::Debug + Clone,
    A: Attestor + 'static + std::fmt::Debug + Clone,
{
    /// Create a new connection pool
    pub fn new(
        transport: Arc<T>,
        our_node_id: NodeId,
        signing_key: SigningKey,
        config: ConnectionPoolConfig,
        incoming_tx: flume::Sender<VerifiedMessage>,
        governance: Arc<G>,
        attestor: Arc<A>,
    ) -> Self {
        let cose_handler = Arc::new(CoseHandler::new(signing_key));
        let attestation_verifier = Arc::new(AttestationVerifier::new(governance, attestor));
        let connection_verifier = Arc::new(ConnectionVerifier::new(
            attestation_verifier,
            cose_handler.clone(),
            our_node_id,
        ));

        Self {
            transport,
            our_node_id,
            cose_handler,
            connection_verifier,
            connections: Arc::new(DashMap::new()),
            connections_by_id: Arc::new(DashMap::new()),
            pending_connections: Arc::new(DashMap::new()),
            _config: config,
            incoming_tx,
            next_conn_id: AtomicUsize::new(0),
        }
    }

    /// Get connection by ID
    pub fn get_connection_by_id(&self, conn_id: &str) -> Option<Arc<Connection<G, A>>> {
        self.connections_by_id.get(conn_id).map(|c| c.clone())
    }

    /// Get or create a connection to a peer
    pub async fn get_connection(&self, peer: &Node) -> NetworkResult<Arc<Connection<G, A>>> {
        let peer_id = *peer.node_id();

        // Retry loop to handle concurrent connection attempts
        let mut retries = 0;
        const MAX_RETRIES: u32 = 10;

        loop {
            // Check if we have an existing verified connection
            if let Some(conns) = self.connections.get(&peer_id) {
                for conn in conns.iter() {
                    // Skip unhealthy connections
                    if !conn.is_healthy() {
                        debug!("Skipping unhealthy connection to {}", peer_id);
                        continue;
                    }

                    // Connection manages its own verification state
                    if let Some(verified_peer) = conn.peer_id().await {
                        if verified_peer == peer_id {
                            return Ok(conn.clone());
                        }
                    }
                }
            }

            // Check if there's a pending connection
            if let Some(pending_entry) = self.pending_connections.get(&peer_id) {
                let mut pending_guard = pending_entry.lock().await;

                if let Some(receiver) = pending_guard.take() {
                    // Drop the lock before waiting
                    drop(pending_guard);
                    drop(pending_entry);

                    debug!("Waiting for pending connection to {}", peer_id);

                    // Wait for the pending connection
                    match receiver.await {
                        Ok(connection) => {
                            debug!("Reusing pending connection to {}", peer_id);
                            return Ok(connection);
                        }
                        Err(_) => {
                            // The connection attempt failed, we'll try again
                            debug!("Pending connection to {} failed, will retry", peer_id);
                            // Continue to next iteration
                        }
                    }
                } else {
                    // Someone else is already creating a connection
                    // Drop locks and retry after a brief wait
                    drop(pending_guard);
                    drop(pending_entry);

                    if retries >= MAX_RETRIES {
                        return Err(NetworkError::ConnectionFailed {
                            node: Box::new(peer_id),
                            reason: "Max retries exceeded waiting for pending connection"
                                .to_string(),
                        });
                    }

                    // Wait a bit for the other connection attempt
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        10 * (retries + 1) as u64,
                    ))
                    .await;
                    retries += 1;
                    continue; // Retry the loop
                }
            }

            // No existing or pending connection, create a new one
            debug!(
                "No existing or pending connection found, creating new one for {}",
                peer_id
            );
            return self.create_outgoing_connection(peer).await;
        }
    }

    /// Create a new outgoing connection
    async fn create_outgoing_connection(
        &self,
        peer: &Node,
    ) -> NetworkResult<Arc<Connection<G, A>>> {
        let peer_id = *peer.node_id();

        // Create a channel for other waiters
        let (tx, rx) = oneshot::channel();

        // Try to register as the one creating the connection
        // Use a scope to ensure the entry is dropped before we try to remove
        {
            let entry = self
                .pending_connections
                .entry(peer_id)
                .or_insert_with(|| Arc::new(Mutex::new(None)));

            let mut guard = entry.lock().await;

            // Check if someone else started creating a connection while we were waiting for the lock
            if let Some(receiver) = guard.take() {
                // Someone else is creating the connection
                drop(guard);
                drop(entry);

                debug!(
                    "Another task is creating connection to {}, waiting",
                    peer_id
                );

                match receiver.await {
                    Ok(connection) => return Ok(connection),
                    Err(_) => {
                        // The other attempt failed, we'll try ourselves
                        debug!(
                            "Other connection attempt to {} failed, trying ourselves",
                            peer_id
                        );
                        // Re-acquire the entry to try again
                        let entry = self
                            .pending_connections
                            .entry(peer_id)
                            .or_insert_with(|| Arc::new(Mutex::new(None)));
                        let mut guard = entry.lock().await;
                        *guard = Some(rx);
                        drop(guard);
                        drop(entry);
                    }
                }
            } else {
                // We're the first ones, set up our receiver
                *guard = Some(rx);
                drop(guard);
                drop(entry);
            }
        }

        // Actually create the connection
        debug!("About to create connection to {}", peer_id);
        let result = self.do_create_connection(peer).await;
        debug!(
            "Connection creation result for {}: {:?}",
            peer_id,
            result.is_ok()
        );

        debug!("About to remove from pending connections");
        // Remove from pending
        self.pending_connections.remove(&peer_id);
        debug!("Removed from pending connections");

        // Notify any waiters
        debug!("About to match on result");
        match result {
            Ok(ref connection) => {
                debug!("Entering Ok branch for {}", peer_id);
                debug!("Notifying waiters of successful connection to {}", peer_id);
                let send_result = tx.send(connection.clone());
                debug!("Notification send result: {:?}", send_result.is_ok());
                debug!(
                    "Returning connection to {} from create_outgoing_connection",
                    peer_id
                );
                Ok(connection.clone())
            }
            Err(ref e) => {
                debug!("Entering Err branch for {}", peer_id);
                debug!("Connection creation failed for {}: {}", peer_id, e);
                // Drop the sender to signal failure
                drop(tx);
                Err(NetworkError::ConnectionFailed {
                    node: Box::new(peer_id),
                    reason: format!("Connection creation failed: {e}"),
                })
            }
        }
    }

    /// Actually create the connection (separated for cleaner error handling)
    async fn do_create_connection(&self, peer: &Node) -> NetworkResult<Arc<Connection<G, A>>> {
        debug!("Creating new connection to {}", peer.node_id());

        let transport_conn =
            self.transport
                .connect(peer)
                .await
                .map_err(|e| NetworkError::ConnectionFailed {
                    node: Box::new(*peer.node_id()),
                    reason: format!("Transport error: {e}"),
                })?;

        let conn_id = format!("conn-{}", self.next_conn_id.fetch_add(1, Ordering::AcqRel));

        let connection = Connection::new_outgoing(
            conn_id.clone(),
            self.our_node_id,
            *peer.node_id(),
            transport_conn,
            self.cose_handler.clone(),
            self.connection_verifier.clone(),
            self.incoming_tx.clone(),
        )
        .await?;

        let connection = Arc::new(connection);

        // Wait for verification to complete
        let verified_peer = connection
            .wait_for_verification(std::time::Duration::from_secs(30))
            .await?;

        // Verify it's the expected peer
        if verified_peer != *peer.node_id() {
            return Err(NetworkError::ConnectionFailed {
                node: Box::new(*peer.node_id()),
                reason: format!("Peer mismatch: expected {peer:?}, got {verified_peer}"),
            });
        }

        // Store connection
        self.connections
            .entry(*peer.node_id())
            .or_default()
            .push(connection.clone());

        // Also store by ID for bidirectional streaming
        self.connections_by_id.insert(conn_id, connection.clone());

        debug!(
            "Successfully created and verified connection to {}",
            peer.node_id()
        );

        Ok(connection)
    }

    /// Send a message to a peer
    pub async fn send(
        &self,
        peer: &Node,
        payload: bytes::Bytes,
        headers: HashMap<String, String>,
    ) -> NetworkResult<()> {
        debug!(
            "ConnectionPool::send - getting connection to {}",
            peer.node_id()
        );
        let conn = match self.get_connection(peer).await {
            Ok(c) => {
                debug!("ConnectionPool::send - successfully got connection");
                c
            }
            Err(e) => {
                debug!("ConnectionPool::send - failed to get connection: {}", e);
                return Err(e);
            }
        };
        debug!(
            "ConnectionPool::send - got connection (type: {}), sending message with headers: {:?}",
            match &conn.as_ref() {
                Connection::Outgoing(_) => "Outgoing",
                Connection::Incoming(_) => "Incoming",
            },
            headers
        );
        debug!(
            "ConnectionPool::send - payload size: {} bytes",
            payload.len()
        );
        let result = conn.send(payload, headers).await;
        debug!("ConnectionPool::send - send result: {:?}", result);

        // If send failed due to closed channel, remove the connection from pool
        if let Err(NetworkError::ChannelClosed(_)) = &result {
            debug!(
                "Connection to {} closed, removing from pool",
                peer.node_id()
            );
            self.remove_connection(peer.node_id(), &conn).await;
        }

        result
    }

    /// Remove a broken connection from the pool
    async fn remove_connection(&self, peer_id: &NodeId, conn: &Arc<Connection<G, A>>) {
        // Remove from peer connections
        if let Some(mut conns) = self.connections.get_mut(peer_id) {
            conns.retain(|c| !Arc::ptr_eq(c, conn));
            if conns.is_empty() {
                drop(conns);
                self.connections.remove(peer_id);
            }
        }

        // Remove from connections by ID if it's there
        self.connections_by_id.remove(conn.id());
    }

    /// Clean up all unhealthy connections
    pub async fn cleanup_unhealthy_connections(&self) {
        let mut to_remove = Vec::new();

        // Find all unhealthy connections
        for entry in self.connections.iter() {
            let peer_id = *entry.key();
            for conn in entry.value().iter() {
                if !conn.is_healthy() {
                    to_remove.push((peer_id, conn.clone()));
                }
            }
        }

        // Remove them
        for (peer_id, conn) in to_remove {
            debug!("Removing unhealthy connection to {}", peer_id);
            self.remove_connection(&peer_id, &conn).await;
        }
    }

    /// Handle an incoming connection
    pub async fn handle_incoming(&self, transport_conn: Box<dyn TransportConnection>) {
        let conn_id = format!(
            "incoming-{}",
            self.next_conn_id.fetch_add(1, Ordering::AcqRel)
        );

        match Connection::new_incoming(
            conn_id.clone(),
            self.our_node_id,
            transport_conn,
            self.cose_handler.clone(),
            self.connection_verifier.clone(),
            self.incoming_tx.clone(),
        )
        .await
        {
            Ok(connection) => {
                let connection = Arc::new(connection);

                // Store by ID first
                self.connections_by_id
                    .insert(conn_id.clone(), connection.clone());

                // Spawn task to wait for verification and update peer mapping
                let connections = self.connections.clone();
                let connections_by_id = self.connections_by_id.clone();

                tokio::spawn(async move {
                    // Wait for verification to complete
                    match connection
                        .wait_for_verification(std::time::Duration::from_secs(30))
                        .await
                    {
                        Ok(peer_id) => {
                            // Add to peer connections
                            connections
                                .entry(peer_id)
                                .or_default()
                                .push(connection.clone());
                        }
                        Err(e) => {
                            warn!("Incoming connection {} verification failed: {}", conn_id, e);
                            // Remove from connections_by_id
                            connections_by_id.remove(&conn_id);
                        }
                    }
                });
            }
            Err(e) => {
                warn!("Failed to create incoming connection: {}", e);
            }
        }
    }
}
