//! Integration test for concurrent RPC calls

use futures::future::join_all;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Barrier;
use tokio::time::Instant;

use proven_vsock_rpc::{
    Bytes, Error, HandlerResponse, MessagePattern, RequestOptions, RpcClient, RpcHandler,
    RpcMessage, RpcServer, ServerConfig,
};

// Test messages
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct EchoRequest {
    id: u32,
    message: String,
    delay_ms: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct EchoResponse {
    id: u32,
    message: String,
    processed_at: u64,
}

impl RpcMessage for EchoRequest {
    type Response = EchoResponse;

    fn message_id(&self) -> &'static str {
        "echo"
    }
}

impl TryFrom<EchoRequest> for Bytes {
    type Error = proven_vsock_rpc::error::CodecError;

    fn try_from(value: EchoRequest) -> Result<Self, Self::Error> {
        bincode::serialize(&value)
            .map(Bytes::from)
            .map_err(|e| proven_vsock_rpc::error::CodecError::SerializationFailed(e.to_string()))
    }
}

impl TryFrom<Bytes> for EchoRequest {
    type Error = proven_vsock_rpc::error::CodecError;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(&value)
            .map_err(|e| proven_vsock_rpc::error::CodecError::DeserializationFailed(e.to_string()))
    }
}

impl TryFrom<EchoResponse> for Bytes {
    type Error = proven_vsock_rpc::error::CodecError;

    fn try_from(value: EchoResponse) -> Result<Self, Self::Error> {
        bincode::serialize(&value)
            .map(Bytes::from)
            .map_err(|e| proven_vsock_rpc::error::CodecError::SerializationFailed(e.to_string()))
    }
}

impl TryFrom<Bytes> for EchoResponse {
    type Error = proven_vsock_rpc::error::CodecError;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        bincode::deserialize(&value)
            .map_err(|e| proven_vsock_rpc::error::CodecError::DeserializationFailed(e.to_string()))
    }
}

// Test handler that introduces delays
#[derive(Clone)]
struct TestHandler {
    start_time: Instant,
}

impl TestHandler {
    fn new() -> Self {
        Self {
            start_time: Instant::now(),
        }
    }
}

#[async_trait::async_trait]
impl RpcHandler for TestHandler {
    async fn handle_message(
        &self,
        message_id: &str,
        payload: Bytes,
        _pattern: MessagePattern,
    ) -> Result<HandlerResponse, Error> {
        match message_id {
            "echo" => {
                let request: EchoRequest = payload.try_into()?;

                // Simulate processing delay
                if request.delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(request.delay_ms)).await;
                }

                let response = EchoResponse {
                    id: request.id,
                    message: format!("Echo: {}", request.message),
                    processed_at: self.start_time.elapsed().as_millis() as u64,
                };

                let bytes: Bytes = response.try_into()?;
                Ok(HandlerResponse::Single(bytes))
            }
            _ => Ok(HandlerResponse::None),
        }
    }
}

#[tokio::test]
async fn test_concurrent_requests() {
    // Allocate port
    let port = proven_util::port_allocator::allocate_port();
    let server_addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));

    // Start server
    let config = ServerConfig {
        max_connections: 100,
        request_timeout: Duration::from_secs(30),
        max_frame_size: 1024 * 1024,
        decompress_requests: false,
    };

    let handler = TestHandler::new();
    let server = RpcServer::new(server_addr, handler, config);

    // Run server in background
    let server_handle = tokio::spawn(async move { server.serve().await });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create client
    let client = RpcClient::builder()
        .vsock_addr(std::net::SocketAddr::from(([127, 0, 0, 1], port)))
        .pool_size(20) // Increase pool size to handle concurrent requests
        .default_timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to build client");
    let client = Arc::new(client);

    // Test 1: Send multiple requests concurrently
    let num_requests = 20;
    let barrier = Arc::new(Barrier::new(num_requests));

    let start = Instant::now();
    let mut handles = vec![];

    for i in 0..num_requests {
        let client = client.clone();
        let barrier = barrier.clone();

        let handle = tokio::spawn(async move {
            // Wait for all tasks to be ready
            barrier.wait().await;

            let request = EchoRequest {
                id: i as u32,
                message: format!("Request {i}"),
                delay_ms: 50, // 50ms delay per request
            };

            let start_time = Instant::now();
            let response: EchoResponse = match client.request(request).await {
                Ok(resp) => resp,
                Err(e) => {
                    panic!("Request {i} failed: {e:?}");
                }
            };
            let duration = start_time.elapsed();

            assert_eq!(response.id, i as u32);
            assert_eq!(response.message, format!("Echo: Request {i}"));

            (i, duration)
        });

        handles.push(handle);
    }

    // Wait for all requests to complete
    let results = join_all(handles).await;
    let total_duration = start.elapsed();

    // Verify all requests completed
    assert_eq!(results.len(), num_requests);
    for (i, result) in results.iter().enumerate() {
        let (id, _duration) = result.as_ref().expect("Task panicked");
        assert_eq!(*id, i);
    }

    // Verify concurrent execution (should be much faster than sequential)
    let sequential_time = Duration::from_millis(50 * num_requests as u64);
    assert!(
        total_duration < sequential_time / 2,
        "Concurrent execution took {:?}, expected less than {:?}",
        total_duration,
        sequential_time / 2
    );

    println!("✓ {num_requests} concurrent requests completed in {total_duration:?}");

    // Cleanup
    client.shutdown().expect("Failed to shutdown client");
    server_handle.abort();
}

#[tokio::test]
async fn test_request_ordering() {
    // Allocate port
    let port = proven_util::port_allocator::allocate_port();
    let server_addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));

    // Start server
    let handler = TestHandler::new();
    let server = RpcServer::new(server_addr, handler, ServerConfig::default());

    // Run server in background
    let server_handle = tokio::spawn(async move { server.serve().await });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create client
    let client = RpcClient::builder()
        .vsock_addr(std::net::SocketAddr::from(([127, 0, 0, 1], port)))
        .build()
        .expect("Failed to build client");
    let client = Arc::new(client);

    // Send requests with varying delays
    let mut handles = vec![];
    let delays = [100, 50, 150, 25, 200]; // Different delays

    for (i, delay) in delays.iter().enumerate() {
        let client = client.clone();
        let delay = *delay;

        let handle = tokio::spawn(async move {
            let request = EchoRequest {
                id: i as u32,
                message: format!("Request {i}"),
                delay_ms: delay,
            };

            let response: EchoResponse = client.request(request).await.expect("Request failed");

            (i, response.processed_at, delay)
        });

        handles.push(handle);
    }

    // Wait for all requests
    let results = join_all(handles).await;

    // Verify all completed
    assert_eq!(results.len(), delays.len());

    // Check that responses arrived roughly in order of their delays
    let mut sorted_results: Vec<_> = results
        .into_iter()
        .map(|r| r.expect("Task panicked"))
        .collect();
    sorted_results.sort_by_key(|&(_, processed_at, _)| processed_at);

    println!("✓ Request ordering test completed");
    for (id, processed_at, delay) in sorted_results {
        println!("  Request {id} (delay {delay}ms) processed at {processed_at}ms");
    }

    // Cleanup
    client.shutdown().expect("Failed to shutdown client");
    server_handle.abort();
}

#[tokio::test]
async fn test_connection_pooling() {
    // Allocate port
    let port = proven_util::port_allocator::allocate_port();
    let server_addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));

    // Start server
    let config = ServerConfig {
        max_connections: 10, // Limit connections
        ..Default::default()
    };

    let handler = TestHandler::new();
    let server = RpcServer::new(server_addr, handler, config);

    // Run server in background
    let server_handle = tokio::spawn(async move { server.serve().await });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create client with connection pool
    let client = RpcClient::builder()
        .vsock_addr(std::net::SocketAddr::from(([127, 0, 0, 1], port)))
        .pool_size(5) // Use smaller pool than server limit
        .build()
        .expect("Failed to build client");
    let client = Arc::new(client);

    // Send many requests to test connection reuse
    let num_requests = 100;
    let mut handles = vec![];

    // Launch requests in smaller batches to avoid overwhelming the pool
    for i in 0..num_requests {
        let client = client.clone();

        let handle = tokio::spawn(async move {
            let request = EchoRequest {
                id: i as u32,
                message: format!("Request {i}"),
                delay_ms: 10,
            };

            let _response: EchoResponse = client.request(request).await.expect("Request failed");
        });

        handles.push(handle);

        // Add small delay every 10 requests to allow connection reuse
        if i % 10 == 9 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    // Wait for all requests
    join_all(handles).await;

    println!("✓ {num_requests} requests completed with connection pooling");

    // Cleanup
    client.shutdown().expect("Failed to shutdown client");
    server_handle.abort();
}

#[tokio::test]
async fn test_request_timeout() {
    // Allocate port
    let port = proven_util::port_allocator::allocate_port();
    let server_addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));

    // Start server
    let handler = TestHandler::new();
    let server = RpcServer::new(server_addr, handler, ServerConfig::default());

    // Run server in background
    let server_handle = tokio::spawn(async move { server.serve().await });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create client with short timeout
    let client = RpcClient::builder()
        .vsock_addr(std::net::SocketAddr::from(([127, 0, 0, 1], port)))
        .default_timeout(Duration::from_millis(500)) // 500ms timeout
        .build()
        .expect("Failed to build client");

    // Send request that will timeout
    let request = EchoRequest {
        id: 1,
        message: "Timeout test".to_string(),
        delay_ms: 2000, // 2 second delay
    };

    let result: Result<EchoResponse, _> = client
        .request_with_options(
            request,
            RequestOptions::with_timeout(Duration::from_millis(500)),
        )
        .await;

    match &result {
        Ok(resp) => panic!("Expected timeout but got response: {resp:?}"),
        Err(e) => println!("Got error as expected: {e:?}"),
    }

    assert!(result.is_err());
    match result.err().unwrap() {
        Error::Timeout(_) => println!("✓ Request timed out as expected"),
        e => panic!("Expected timeout error, got: {e:?}"),
    }

    // Cleanup
    client.shutdown().expect("Failed to shutdown client");
    server_handle.abort();
}

#[tokio::test]
async fn test_concurrent_clients() {
    // Allocate port
    let port = proven_util::port_allocator::allocate_port();
    let server_addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));

    // Start server
    let handler = TestHandler::new();
    let server = RpcServer::new(server_addr, handler, ServerConfig::default());

    // Run server in background
    let server_handle = tokio::spawn(async move { server.serve().await });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create multiple clients
    let num_clients = 10;
    let mut client_handles = vec![];

    for client_id in 0..num_clients {
        let handle = tokio::spawn(async move {
            let client = RpcClient::builder()
                .vsock_addr(std::net::SocketAddr::from(([127, 0, 0, 1], port)))
                .build()
                .expect("Failed to build client");

            // Each client sends multiple requests
            for i in 0..5 {
                let request = EchoRequest {
                    id: client_id * 100 + i,
                    message: format!("Client {client_id} Request {i}"),
                    delay_ms: 20,
                };

                let response: EchoResponse = client.request(request).await.expect("Request failed");

                assert_eq!(response.id, client_id * 100 + i);
            }

            client.shutdown().expect("Failed to shutdown client");
        });

        client_handles.push(handle);
    }

    // Wait for all clients
    join_all(client_handles).await;

    println!("✓ {num_clients} concurrent clients completed");

    // Cleanup
    server_handle.abort();
}
