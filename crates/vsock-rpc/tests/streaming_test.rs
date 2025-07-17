//! Integration tests for streaming functionality in vsock-rpc

use bytes::Bytes;
use futures::stream::StreamExt;
use proven_vsock_rpc::{
    HandlerResponse, MessagePattern, RpcClient, RpcHandler, RpcMessage, RpcServer, ServerConfig,
};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::time::Duration;
use tokio::sync::mpsc;

// Test message types for streaming
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StreamRequest {
    count: u32,
    delay_ms: u64,
}

impl RpcMessage for StreamRequest {
    type Response = StreamItem;

    fn message_id(&self) -> &'static str {
        "stream_request"
    }
}

impl TryFrom<StreamRequest> for Bytes {
    type Error = bincode::Error;

    fn try_from(value: StreamRequest) -> std::result::Result<Self, Self::Error> {
        bincode::serialize(&value).map(Bytes::from)
    }
}

impl TryFrom<Bytes> for StreamRequest {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StreamItem {
    index: u32,
    data: String,
}

impl TryFrom<StreamItem> for Bytes {
    type Error = bincode::Error;

    fn try_from(value: StreamItem) -> std::result::Result<Self, Self::Error> {
        bincode::serialize(&value).map(Bytes::from)
    }
}

impl TryFrom<Bytes> for StreamItem {
    type Error = bincode::Error;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        bincode::deserialize(&value)
    }
}

// Test handler that produces streams
struct StreamingHandler;

#[async_trait::async_trait]
impl RpcHandler for StreamingHandler {
    async fn handle_message(
        &self,
        message_id: &str,
        message: Bytes,
        pattern: MessagePattern,
    ) -> proven_vsock_rpc::Result<HandlerResponse> {
        eprintln!(
            "Handler received message_id: {}, pattern: {:?}",
            message_id, pattern
        );
        match message_id {
            "stream_request" => {
                let request: StreamRequest = bincode::deserialize(&message).map_err(|e| {
                    proven_vsock_rpc::error::Error::Codec(
                        proven_vsock_rpc::error::CodecError::DeserializationFailed(e.to_string()),
                    )
                })?;

                eprintln!(
                    "Decoded request: count={}, delay_ms={}",
                    request.count, request.delay_ms
                );

                match pattern {
                    MessagePattern::RequestStream { .. } => {
                        eprintln!("Pattern is RequestStream, creating stream...");
                        // Create a channel for streaming responses
                        let (tx, rx) = mpsc::channel(32);

                        // Spawn task to produce stream items
                        tokio::spawn(async move {
                            eprintln!(
                                "Stream producer task started, will send {} items",
                                request.count
                            );
                            for i in 0..request.count {
                                let item = StreamItem {
                                    index: i,
                                    data: format!("Stream item {i}"),
                                };

                                eprintln!("Producing item {i}");

                                if request.delay_ms > 0 {
                                    tokio::time::sleep(Duration::from_millis(request.delay_ms))
                                        .await;
                                }

                                match bincode::serialize(&item).map(Bytes::from) {
                                    Ok(bytes) => {
                                        eprintln!("Sending item {} ({} bytes)", i, bytes.len());
                                        if tx.send(Ok(bytes)).await.is_err() {
                                            eprintln!("Failed to send item {i} - channel closed");
                                            break;
                                        }
                                        eprintln!("Item {i} sent successfully");
                                    }
                                    Err(e) => {
                                        eprintln!("Failed to serialize stream item: {e}");
                                        break;
                                    }
                                }
                            }
                            eprintln!("Stream producer task completed");
                        });

                        Ok(HandlerResponse::Stream(rx))
                    }
                    _ => {
                        // For non-streaming requests, just return the first item
                        let item = StreamItem {
                            index: 0,
                            data: "Single response".to_string(),
                        };
                        let bytes = bincode::serialize(&item).map(Bytes::from).map_err(|e| {
                            proven_vsock_rpc::error::Error::Codec(
                                proven_vsock_rpc::error::CodecError::SerializationFailed(
                                    e.to_string(),
                                ),
                            )
                        })?;
                        Ok(HandlerResponse::Single(bytes))
                    }
                }
            }
            _ => Err(proven_vsock_rpc::error::Error::Handler(
                proven_vsock_rpc::error::HandlerError::NotFound(format!(
                    "Unknown message: {message_id}"
                )),
            )),
        }
    }
}

#[tokio::test]
async fn test_streaming_basic() {
    // Start server on a random high port
    let port = proven_util::port_allocator::allocate_port();
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let server = RpcServer::new(addr, StreamingHandler, ServerConfig::default());

    eprintln!("Starting server on {addr:?}");
    let server_handle = tokio::spawn(async move {
        eprintln!("Server starting...");
        let result = server.serve().await;
        eprintln!("Server ended with result: {result:?}");
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create client
    let client = RpcClient::builder().vsock_addr(addr).build().unwrap();

    // Request a stream
    let request = StreamRequest {
        count: 5,
        delay_ms: 10,
    };

    eprintln!("Sending stream request...");
    let stream_result = client.request_stream(request).await;
    match stream_result {
        Ok(mut stream) => {
            eprintln!("Got stream, waiting for items...");

            // Collect stream items
            let mut items = Vec::new();
            while let Some(result) = stream.next().await {
                eprintln!("Got stream result...");
                match result {
                    Ok(item) => {
                        eprintln!("Got item: {item:?}");
                        items.push(item);
                    }
                    Err(e) => panic!("Stream error: {e}"),
                }
            }
            eprintln!("Stream ended");

            // Verify we received all items
            assert_eq!(items.len(), 5);
            for (i, item) in items.iter().enumerate() {
                assert_eq!(item.index, i as u32);
                assert_eq!(item.data, format!("Stream item {i}"));
            }
        }
        Err(e) => {
            panic!("Failed to create stream: {e:?}");
        }
    }

    // Cleanup
    client.shutdown().unwrap();
    server_handle.abort();
}

#[tokio::test]
async fn test_streaming_large_payloads() {
    // Start server on a random high port
    let port = proven_util::port_allocator::allocate_port();
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));

    struct LargePayloadHandler;

    #[async_trait::async_trait]
    impl RpcHandler for LargePayloadHandler {
        async fn handle_message(
            &self,
            message_id: &str,
            _message: Bytes,
            pattern: MessagePattern,
        ) -> proven_vsock_rpc::Result<HandlerResponse> {
            match (message_id, pattern) {
                ("stream_request", MessagePattern::RequestStream { .. }) => {
                    let (tx, rx) = mpsc::channel(8);

                    tokio::spawn(async move {
                        // Send 10 chunks of 64KB each
                        for i in 0..10 {
                            let item = StreamItem {
                                index: i,
                                data: "X".repeat(64 * 1024), // 64KB of data
                            };

                            match bincode::serialize(&item).map(Bytes::from) {
                                Ok(bytes) => {
                                    if tx.send(Ok(bytes)).await.is_err() {
                                        break;
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                    });

                    Ok(HandlerResponse::Stream(rx))
                }
                _ => Err(proven_vsock_rpc::error::Error::Handler(
                    proven_vsock_rpc::error::HandlerError::NotFound(format!(
                        "Unknown message: {message_id}"
                    )),
                )),
            }
        }
    }

    let server = RpcServer::new(addr, LargePayloadHandler, ServerConfig::default());

    let server_handle = tokio::spawn(async move {
        let _ = server.serve().await;
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create client
    let client = RpcClient::builder().vsock_addr(addr).build().unwrap();

    // Request a stream
    let request = StreamRequest {
        count: 10,
        delay_ms: 0,
    };

    let mut stream = client.request_stream(request).await.unwrap();

    // Collect stream items and verify sizes
    let mut total_bytes = 0;
    let mut count = 0;

    while let Some(result) = stream.next().await {
        match result {
            Ok(item) => {
                assert_eq!(item.data.len(), 64 * 1024);
                total_bytes += item.data.len();
                count += 1;
            }
            Err(e) => panic!("Stream error: {e}"),
        }
    }

    assert_eq!(count, 10);
    assert_eq!(total_bytes, 10 * 64 * 1024);

    // Cleanup
    client.shutdown().unwrap();
    server_handle.abort();
}

#[tokio::test]
async fn test_streaming_backpressure() {
    // Start server on a random high port
    let port = proven_util::port_allocator::allocate_port();
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));

    struct BackpressureHandler;

    #[async_trait::async_trait]
    impl RpcHandler for BackpressureHandler {
        async fn handle_message(
            &self,
            message_id: &str,
            _message: Bytes,
            pattern: MessagePattern,
        ) -> proven_vsock_rpc::Result<HandlerResponse> {
            match (message_id, pattern) {
                ("stream_request", MessagePattern::RequestStream { .. }) => {
                    let (tx, rx) = mpsc::channel(2); // Small buffer to test backpressure

                    tokio::spawn(async move {
                        // Try to send many items quickly
                        for i in 0..100 {
                            let item = StreamItem {
                                index: i,
                                data: format!("Item {i}"),
                            };

                            match bincode::serialize(&item).map(Bytes::from) {
                                Ok(bytes) => {
                                    // This should block when buffer is full
                                    if tx.send(Ok(bytes)).await.is_err() {
                                        eprintln!("Client disconnected at item {i}");
                                        break;
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                        eprintln!("Server finished sending items");
                    });

                    Ok(HandlerResponse::Stream(rx))
                }
                _ => Err(proven_vsock_rpc::error::Error::Handler(
                    proven_vsock_rpc::error::HandlerError::NotFound(format!(
                        "Unknown message: {message_id}"
                    )),
                )),
            }
        }
    }

    let server = RpcServer::new(addr, BackpressureHandler, ServerConfig::default());

    let server_handle = tokio::spawn(async move {
        let _ = server.serve().await;
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create client
    let client = RpcClient::builder().vsock_addr(addr).build().unwrap();

    // Request a stream
    let request = StreamRequest {
        count: 100,
        delay_ms: 0,
    };

    let mut stream = client.request_stream(request).await.unwrap();

    // Slowly consume items to test backpressure
    let mut count = 0;
    while let Some(result) = stream.next().await {
        match result {
            Ok(item) => {
                assert_eq!(item.index, count);
                count += 1;

                // Slow consumer
                tokio::time::sleep(Duration::from_millis(10)).await;

                // Only consume first 20 items
                if count >= 20 {
                    break;
                }
            }
            Err(e) => panic!("Stream error: {e}"),
        }
    }

    assert_eq!(count, 20);

    // Cleanup
    drop(stream); // Drop stream to signal we're done
    client.shutdown().unwrap();
    server_handle.abort();
}

#[tokio::test]
async fn test_streaming_error_handling() {
    // Start server on a random high port
    let port = proven_util::port_allocator::allocate_port();
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));

    struct ErrorHandler;

    #[async_trait::async_trait]
    impl RpcHandler for ErrorHandler {
        async fn handle_message(
            &self,
            message_id: &str,
            _message: Bytes,
            pattern: MessagePattern,
        ) -> proven_vsock_rpc::Result<HandlerResponse> {
            match (message_id, pattern) {
                ("stream_request", MessagePattern::RequestStream { .. }) => {
                    let (tx, rx) = mpsc::channel(32);

                    tokio::spawn(async move {
                        // Send a few successful items
                        for i in 0..3 {
                            let item = StreamItem {
                                index: i,
                                data: format!("Item {i}"),
                            };

                            match bincode::serialize(&item).map(Bytes::from) {
                                Ok(bytes) => {
                                    if tx.send(Ok(bytes)).await.is_err() {
                                        break;
                                    }
                                }
                                Err(_) => break,
                            }
                        }

                        // Then send an error
                        let _ = tx
                            .send(Err(proven_vsock_rpc::error::Error::Handler(
                                proven_vsock_rpc::error::HandlerError::Internal(
                                    "Simulated stream error".to_string(),
                                ),
                            )))
                            .await;
                    });

                    Ok(HandlerResponse::Stream(rx))
                }
                _ => Err(proven_vsock_rpc::error::Error::Handler(
                    proven_vsock_rpc::error::HandlerError::NotFound(format!(
                        "Unknown message: {message_id}"
                    )),
                )),
            }
        }
    }

    let server = RpcServer::new(addr, ErrorHandler, ServerConfig::default());

    let server_handle = tokio::spawn(async move {
        let _ = server.serve().await;
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create client
    let client = RpcClient::builder().vsock_addr(addr).build().unwrap();

    // Request a stream
    let request = StreamRequest {
        count: 10,
        delay_ms: 0,
    };

    let mut stream = client.request_stream(request).await.unwrap();

    // Collect items until error
    let mut items = Vec::new();
    let mut got_error = false;

    while let Some(result) = stream.next().await {
        match result {
            Ok(item) => items.push(item),
            Err(_) => {
                got_error = true;
                break;
            }
        }
    }

    assert_eq!(items.len(), 3);
    assert!(got_error);

    // Cleanup
    client.shutdown().unwrap();
    server_handle.abort();
}

#[tokio::test]
async fn test_concurrent_streams() {
    // Start server on a random high port
    let port = proven_util::port_allocator::allocate_port();
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let server = RpcServer::new(addr, StreamingHandler, ServerConfig::default());

    let server_handle = tokio::spawn(async move {
        let _ = server.serve().await;
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create client
    let client = std::sync::Arc::new(RpcClient::builder().vsock_addr(addr).build().unwrap());

    // Start multiple concurrent streams
    let mut handles = Vec::new();

    for stream_id in 0..5 {
        let client_clone = client.clone();
        let handle = tokio::spawn(async move {
            let request = StreamRequest {
                count: 10,
                delay_ms: 5,
            };

            let mut stream = client_clone.request_stream(request).await.unwrap();
            let mut count = 0;

            while let Some(result) = stream.next().await {
                match result {
                    Ok(_) => count += 1,
                    Err(e) => panic!("Stream {stream_id} error: {e}"),
                }
            }

            assert_eq!(count, 10);
            stream_id
        });

        handles.push(handle);
    }

    // Wait for all streams to complete
    for handle in handles {
        let stream_id = handle.await.unwrap();
        eprintln!("Stream {stream_id} completed");
    }

    // Cleanup
    client.shutdown().unwrap();
    server_handle.abort();
}
