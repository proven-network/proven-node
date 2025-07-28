//! WebSocket transport implementation for the new transport trait
//!
//! This implementation provides WebSocket connectivity with a unique approach:
//! - For listening: Provides an Axum handler that can be mounted into an existing server
//! - For connecting: Uses standard WebSocket client connections

use async_trait::async_trait;
use axum::extract::{WebSocketUpgrade, ws::WebSocket};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use proven_topology::Node;
use proven_transport::{Connection, Listener, Transport, TransportError};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info};
use url::Url;

/// WebSocket transport implementation
#[derive(Debug, Clone)]
pub struct WebSocketTransport {
    /// Multiple listeners can be created from this transport
    listeners: Arc<tokio::sync::RwLock<Vec<WebSocketListener>>>,
}

impl WebSocketTransport {
    /// Create a new WebSocket transport with options
    pub fn new() -> Self {
        Self {
            listeners: Arc::new(tokio::sync::RwLock::new(Vec::new())),
        }
    }
}

impl Default for WebSocketTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transport for WebSocketTransport {
    async fn connect(&self, node: &Node) -> Result<Box<dyn Connection>, TransportError> {
        // Get the WebSocket URL from the node
        let url = node
            .websocket_url()
            .map_err(TransportError::InvalidAddress)?;

        let url = Url::parse(&url)
            .map_err(|e| TransportError::InvalidAddress(format!("Invalid URL: {e}")))?;

        debug!("Connecting to WebSocket at {}", url);

        let (ws_stream, _) = connect_async(url.as_str()).await.map_err(|e| {
            TransportError::ConnectionFailed(format!("WebSocket connect failed: {e}"))
        })?;

        let conn = WebSocketClientConnection::new(ws_stream);

        Ok(Box::new(conn))
    }

    async fn listen(&self) -> Result<Box<dyn Listener>, TransportError> {
        // For WebSocket, we don't actually bind a socket here
        // Instead, we create a listener that will receive connections from the Axum handler
        let (connection_tx, connection_rx) = mpsc::channel(100);

        let listener = WebSocketListener {
            connection_rx: Arc::new(tokio::sync::Mutex::new(connection_rx)),
            connection_tx,
        };

        // Store the listener
        self.listeners.write().await.push(listener.clone());

        info!(
            "WebSocket listener created (total: {})",
            self.listeners.read().await.len()
        );

        Ok(Box::new(listener))
    }
}

/// WebSocket listener that receives connections from the Axum handler
#[derive(Debug, Clone)]
pub struct WebSocketListener {
    connection_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<Box<dyn Connection>>>>,
    connection_tx: mpsc::Sender<Box<dyn Connection>>,
}

#[async_trait]
impl Listener for WebSocketListener {
    async fn accept(&self) -> Result<Box<dyn Connection>, TransportError> {
        let mut rx = self.connection_rx.lock().await;
        rx.recv().await.ok_or(TransportError::ConnectionClosed)
    }

    async fn close(self: Box<Self>) -> Result<(), TransportError> {
        // Cleanup is handled by dropping the receiver
        Ok(())
    }
}

/// WebSocket connection for client-side connections
struct WebSocketClientConnection {
    ws_stream: Arc<
        tokio::sync::Mutex<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        >,
    >,
    closed: Arc<tokio::sync::RwLock<bool>>,
}

impl WebSocketClientConnection {
    fn new(
        ws_stream: tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Self {
        Self {
            ws_stream: Arc::new(tokio::sync::Mutex::new(ws_stream)),
            closed: Arc::new(tokio::sync::RwLock::new(false)),
        }
    }
}

impl Debug for WebSocketClientConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketClientConnection")
            .field("closed", &self.closed)
            .finish()
    }
}

#[async_trait]
impl Connection for WebSocketClientConnection {
    async fn send(&self, data: Bytes) -> Result<(), TransportError> {
        if *self.closed.read().await {
            return Err(TransportError::ConnectionClosed);
        }

        let mut stream = self.ws_stream.lock().await;
        stream
            .send(Message::Binary(data))
            .await
            .map_err(|e| TransportError::Io(std::io::Error::other(e)))?;

        Ok(())
    }

    async fn recv(&self) -> Result<Bytes, TransportError> {
        if *self.closed.read().await {
            return Err(TransportError::ConnectionClosed);
        }

        let mut stream = self.ws_stream.lock().await;
        match stream.next().await {
            Some(Ok(Message::Binary(data))) => Ok(data),
            Some(Ok(Message::Close(_))) => {
                *self.closed.write().await = true;
                Err(TransportError::ConnectionClosed)
            }
            Some(Ok(_)) => {
                // Ignore other message types (Text, Ping, Pong)
                Box::pin(self.recv()).await
            }
            Some(Err(e)) => Err(TransportError::Io(std::io::Error::other(e))),
            None => {
                *self.closed.write().await = true;
                Err(TransportError::ConnectionClosed)
            }
        }
    }

    async fn close(self: Box<Self>) -> Result<(), TransportError> {
        *self.closed.write().await = true;
        let mut stream = self.ws_stream.lock().await;
        let _ = stream.close(None).await;
        Ok(())
    }
}

/// WebSocket connection for server-side connections (from Axum)
struct WebSocketServerConnection {
    socket: Arc<tokio::sync::Mutex<WebSocket>>,
    closed: Arc<tokio::sync::RwLock<bool>>,
}

impl WebSocketServerConnection {
    fn new(socket: WebSocket) -> Self {
        Self {
            socket: Arc::new(tokio::sync::Mutex::new(socket)),
            closed: Arc::new(tokio::sync::RwLock::new(false)),
        }
    }
}

impl Debug for WebSocketServerConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketServerConnection")
            .field("closed", &self.closed)
            .finish()
    }
}

#[async_trait]
impl Connection for WebSocketServerConnection {
    async fn send(&self, data: Bytes) -> Result<(), TransportError> {
        if *self.closed.read().await {
            return Err(TransportError::ConnectionClosed);
        }

        let mut socket = self.socket.lock().await;
        socket
            .send(axum::extract::ws::Message::Binary(data))
            .await
            .map_err(|e| TransportError::Io(std::io::Error::other(e.to_string())))?;

        Ok(())
    }

    async fn recv(&self) -> Result<Bytes, TransportError> {
        if *self.closed.read().await {
            return Err(TransportError::ConnectionClosed);
        }

        let mut socket = self.socket.lock().await;
        match socket.recv().await {
            Some(Ok(axum::extract::ws::Message::Binary(data))) => Ok(data),
            Some(Ok(axum::extract::ws::Message::Close(_))) => {
                *self.closed.write().await = true;
                Err(TransportError::ConnectionClosed)
            }
            Some(Ok(_)) => {
                // Ignore other message types
                Box::pin(self.recv()).await
            }
            Some(Err(e)) => Err(TransportError::Io(std::io::Error::other(e.to_string()))),
            None => {
                *self.closed.write().await = true;
                Err(TransportError::ConnectionClosed)
            }
        }
    }

    async fn close(self: Box<Self>) -> Result<(), TransportError> {
        *self.closed.write().await = true;
        // Axum WebSocket doesn't have a close method, it closes when dropped
        Ok(())
    }
}

/// Extension methods for integrating with Axum
impl WebSocketTransport {
    /// Get all current listeners
    pub async fn get_listeners(&self) -> Vec<WebSocketListener> {
        self.listeners.read().await.clone()
    }

    /// Mount a WebSocket endpoint into an Axum router
    ///
    /// Uses the first available listener to receive connections from the Axum handler.
    /// If no listener exists, returns an error.
    pub async fn mount_into_router(
        &self,
        router: axum::Router,
    ) -> Result<axum::Router, TransportError> {
        const WS_PATH: &str = "/consensus/ws";

        // Get the first listener's connection_tx
        let listeners = self.listeners.read().await;
        if listeners.is_empty() {
            return Err(TransportError::Other(
                "No listener available. Call listen() first.".to_string(),
            ));
        }
        let connection_tx = listeners[0].connection_tx.clone();
        drop(listeners);

        // Create the handler inline to avoid complex type signatures
        let handler = move |ws: WebSocketUpgrade| {
            let connection_tx = connection_tx.clone();

            async move {
                ws.on_upgrade(move |socket| async move {
                    let conn = WebSocketServerConnection::new(socket);
                    if connection_tx.send(Box::new(conn)).await.is_err() {
                        error!("Failed to send connection to listener");
                    }
                })
            }
        };

        let router = router.route(WS_PATH, axum::routing::get(handler));

        Ok(router)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_websocket_transport_creation() {
        let _ = tracing_subscriber::fmt::try_init();

        let transport = WebSocketTransport::new();
        // Transport starts with no listeners
        assert!(transport.listeners.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_listener_creation() {
        let _ = tracing_subscriber::fmt::try_init();

        let transport = WebSocketTransport::new();

        let _listener = transport.listen().await.unwrap();

        // Verify listener is registered
        assert_eq!(transport.listeners.read().await.len(), 1);
    }
}
