//! TCP transport implementation
//!
//! This crate provides a TCP implementation of the minimal Transport trait.
//! It handles only the raw TCP connection logic - no framing, no verification,
//! just moving bytes.

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use proven_topology::Node;
use proven_transport::{Connection, Listener, Transport, TransportError};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

/// Configuration for TCP transport
#[derive(Debug, Clone)]
pub struct TcpOptions {
    /// Address to listen on (if acting as a listener)
    pub listen_addr: Option<SocketAddr>,
}

/// TCP transport implementation
#[derive(Debug, Clone)]
pub struct TcpTransport {
    options: TcpOptions,
}

impl TcpTransport {
    /// Create a new TCP transport with options
    pub fn new(options: TcpOptions) -> Self {
        Self { options }
    }

    /// Create a new TCP transport with default options
    pub fn new_default() -> Self {
        Self::new(TcpOptions { listen_addr: None })
    }
}

impl Default for TcpTransport {
    fn default() -> Self {
        Self::new_default()
    }
}

#[async_trait]
impl Transport for TcpTransport {
    async fn connect(&self, node: &Node) -> Result<Box<dyn Connection>, TransportError> {
        // Get the TCP socket address from the node
        let addr = node
            .tcp_socket_addr()
            .await
            .map_err(TransportError::InvalidAddress)?;

        let stream = TcpStream::connect(addr).await?;
        Ok(Box::new(TcpConnection {
            stream: Arc::new(Mutex::new(stream)),
        }))
    }

    async fn listen(&self) -> Result<Box<dyn Listener>, TransportError> {
        let addr = self.options.listen_addr.ok_or_else(|| {
            TransportError::InvalidAddress("No listen address configured".to_string())
        })?;

        let listener = TcpListener::bind(addr).await?;
        Ok(Box::new(TcpListenerWrapper {
            listener: Arc::new(listener),
        }))
    }
}

/// A TCP connection
#[derive(Debug)]
struct TcpConnection {
    stream: Arc<Mutex<TcpStream>>,
}

#[async_trait]
impl Connection for TcpConnection {
    async fn send(&self, data: Bytes) -> Result<(), TransportError> {
        let mut stream = self.stream.lock().await;

        // Simple length-prefixed framing: 4 bytes length + data
        let len = data.len() as u32;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(&data).await?;
        stream.flush().await?;

        Ok(())
    }

    async fn recv(&self) -> Result<Bytes, TransportError> {
        let mut stream = self.stream.lock().await;

        // Read length prefix
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;

        let len = u32::from_be_bytes(len_buf) as usize;
        if len > 10 * 1024 * 1024 {
            // 10MB max
            return Err(TransportError::Other("Message too large".to_string()));
        }

        // Read data
        let mut buf = BytesMut::with_capacity(len);
        buf.resize(len, 0);
        stream
            .read_exact(&mut buf)
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;

        Ok(buf.freeze())
    }

    async fn close(self: Box<Self>) -> Result<(), TransportError> {
        let mut stream = self.stream.lock().await;
        stream.shutdown().await?;
        Ok(())
    }
}

/// A TCP listener wrapper
#[derive(Debug)]
struct TcpListenerWrapper {
    listener: Arc<TcpListener>,
}

#[async_trait]
impl Listener for TcpListenerWrapper {
    async fn accept(&self) -> Result<Box<dyn Connection>, TransportError> {
        let (stream, _remote_addr) = self.listener.accept().await?;
        Ok(Box::new(TcpConnection {
            stream: Arc::new(Mutex::new(stream)),
        }))
    }

    async fn close(self: Box<Self>) -> Result<(), TransportError> {
        // Dropping the listener closes it
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_topology::{Node, NodeId};
    use std::collections::HashSet;

    fn create_test_node(addr: SocketAddr) -> Node {
        Node::new(
            "test-az".to_string(),
            format!("http://{addr}"),
            NodeId::from_seed(1),
            "test-region".to_string(),
            HashSet::new(),
        )
    }

    #[tokio::test]
    async fn test_tcp_transport_listen_connect() {
        // Use a random port
        let listen_addr = "127.0.0.1:0".parse().unwrap();
        let listener_transport = TcpTransport::new(TcpOptions {
            listen_addr: Some(listen_addr),
        });

        // Start listener
        listener_transport.listen().await.unwrap();

        // Get actual bound address (since we used port 0)
        // For this test, we'll use a fixed port instead
        let test_addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let listener_transport = TcpTransport::new(TcpOptions {
            listen_addr: Some(test_addr),
        });
        let listener = listener_transport.listen().await.unwrap();

        // Create a node for connection
        let node = create_test_node(test_addr);

        // Connect to it
        let connect_transport = TcpTransport::new_default();
        let _conn = connect_transport.connect(&node).await.unwrap();

        // Accept connection
        let _server_conn = listener.accept().await.unwrap();
    }

    #[tokio::test]
    async fn test_tcp_send_recv() {
        // Use a fixed port for testing
        let test_addr: SocketAddr = "127.0.0.1:12346".parse().unwrap();
        let listener_transport = TcpTransport::new(TcpOptions {
            listen_addr: Some(test_addr),
        });

        // Start listener
        let listener = listener_transport.listen().await.unwrap();

        // Create a node for connection
        let node = create_test_node(test_addr);

        // Connect
        let connect_transport = TcpTransport::new_default();
        let client = connect_transport.connect(&node).await.unwrap();
        let server = listener.accept().await.unwrap();

        // Send from client to server
        let msg = Bytes::from("Hello, TCP!");
        client.send(msg.clone()).await.unwrap();

        let received = server.recv().await.unwrap();
        assert_eq!(received, msg);

        // Send from server to client
        let reply = Bytes::from("Hello back!");
        server.send(reply.clone()).await.unwrap();

        let received = client.recv().await.unwrap();
        assert_eq!(received, reply);
    }
}
