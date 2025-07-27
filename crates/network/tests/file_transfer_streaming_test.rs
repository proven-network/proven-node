//! Simulated file transfer using streaming
//!
//! This test demonstrates a more realistic use case for streaming:
//! transferring a large file in chunks with progress tracking.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use flume::bounded;
use proven_network::stream::{FlowController, Stream, StreamHandle, StreamReceiver, StreamSender};
use proven_topology::NodeId;
use tracing::info;
use uuid::Uuid;

const CHUNK_SIZE: usize = 8192; // 8KB chunks
const FILE_SIZE: usize = 100 * 1024; // 100KB file for faster testing

#[tokio::test]
async fn test_file_transfer_streaming() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    // Create mock node IDs
    let sender_node = NodeId::from_seed(1);
    let receiver_node = NodeId::from_seed(2);

    // Create channels with reasonable buffer size
    let (tx1, rx1) = bounded::<Bytes>(32); // Buffer up to 32 chunks
    let (tx2, rx2) = bounded::<Bytes>(32);

    // Create flow controllers with window large enough for entire file
    let window_size = FILE_SIZE as u32 * 2; // Double the file size to avoid blocking
    let flow_control1 = Arc::new(FlowController::new(window_size));
    let flow_control2 = Arc::new(FlowController::new(window_size));

    // Create stream handles
    let stream_id = Uuid::new_v4();

    let sender_stream = Stream {
        handle: StreamHandle {
            id: stream_id,
            peer: receiver_node.clone(),
            stream_type: "file_transfer".to_string(),
            sender: StreamSender::new(tx1, flow_control1.clone()),
            receiver: StreamReceiver::new(rx2, flow_control1.clone()),
            flow_control: flow_control1,
        },
    };

    let receiver_stream = Stream {
        handle: StreamHandle {
            id: stream_id,
            peer: sender_node.clone(),
            stream_type: "file_transfer".to_string(),
            sender: StreamSender::new(tx2, flow_control2.clone()),
            receiver: StreamReceiver::new(rx1, flow_control2.clone()),
            flow_control: flow_control2,
        },
    };

    info!(
        "Starting file transfer from {} to {}",
        sender_node, receiver_node
    );

    // Simulate file data
    let file_data: Vec<u8> = (0..FILE_SIZE).map(|i| (i % 256) as u8).collect();
    let file_checksum: u64 = file_data.iter().map(|&b| b as u64).sum();

    info!(
        "File size: {} bytes, checksum: {}",
        FILE_SIZE, file_checksum
    );

    // Progress tracking
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let bytes_received = Arc::new(AtomicU64::new(0));

    // Sender task - sends file in chunks
    let bytes_sent_clone = bytes_sent.clone();
    let sender_task = tokio::spawn(async move {
        let start = Instant::now();
        let mut chunks_sent = 0;

        for chunk in file_data.chunks(CHUNK_SIZE) {
            let chunk_bytes = Bytes::from(chunk.to_vec());
            sender_stream
                .send(chunk_bytes.clone())
                .await
                .expect("Failed to send chunk");

            bytes_sent_clone.fetch_add(chunk.len() as u64, Ordering::Relaxed);
            chunks_sent += 1;

            if chunks_sent % 4 == 0 {
                let progress =
                    bytes_sent_clone.load(Ordering::Relaxed) as f64 / FILE_SIZE as f64 * 100.0;
                info!("Sender progress: {:.1}%", progress);
            }
        }

        // Send end-of-file marker
        sender_stream
            .send(Bytes::new())
            .await
            .expect("Failed to send EOF");

        let elapsed = start.elapsed();
        let throughput = (FILE_SIZE as f64 / elapsed.as_secs_f64()) / (1024.0 * 1024.0);
        info!(
            "Sender completed in {:?}, throughput: {:.2} MB/s",
            elapsed, throughput
        );
    });

    // Receiver task - receives file chunks and verifies
    let bytes_received_clone = bytes_received.clone();
    let receiver_task = tokio::spawn(async move {
        let start = Instant::now();
        let mut received_data = Vec::with_capacity(FILE_SIZE);
        let mut chunks_received = 0;

        loop {
            match receiver_stream.recv().await {
                Some(chunk) => {
                    if chunk.is_empty() {
                        // EOF marker
                        break;
                    }

                    received_data.extend_from_slice(&chunk);
                    bytes_received_clone.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                    chunks_received += 1;

                    if chunks_received % 4 == 0 {
                        let progress = bytes_received_clone.load(Ordering::Relaxed) as f64
                            / FILE_SIZE as f64
                            * 100.0;
                        info!("Receiver progress: {:.1}%", progress);
                    }
                }
                None => {
                    info!("Stream closed unexpectedly");
                    break;
                }
            }
        }

        let elapsed = start.elapsed();
        let throughput = (received_data.len() as f64 / elapsed.as_secs_f64()) / (1024.0 * 1024.0);
        info!(
            "Receiver completed in {:?}, throughput: {:.2} MB/s",
            elapsed, throughput
        );

        // Verify received data
        let received_checksum: u64 = received_data.iter().map(|&b| b as u64).sum();
        (received_data, received_checksum)
    });

    // Wait for both tasks to complete
    sender_task.await.unwrap();
    let (received_data, received_checksum) = receiver_task.await.unwrap();

    // Verify transfer
    assert_eq!(
        received_data.len(),
        FILE_SIZE,
        "Received file size mismatch"
    );
    assert_eq!(received_checksum, file_checksum, "File checksum mismatch");

    let total_sent = bytes_sent.load(Ordering::Relaxed);
    let total_received = bytes_received.load(Ordering::Relaxed);
    assert_eq!(total_sent, total_received, "Bytes sent/received mismatch");

    info!("File transfer test completed successfully!");
    info!("Total bytes transferred: {}", total_sent);
}

#[tokio::test]
async fn test_bidirectional_streaming() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    // Create mock node IDs
    let node1 = NodeId::from_seed(1);
    let node2 = NodeId::from_seed(2);

    // Create bidirectional channels
    let (tx1_to_2, rx1_to_2) = bounded::<Bytes>(100);
    let (tx2_to_1, rx2_to_1) = bounded::<Bytes>(100);

    // Create flow controllers
    let flow_control1 = Arc::new(FlowController::new(65536));
    let flow_control2 = Arc::new(FlowController::new(65536));

    // Create stream handles
    let stream_id = Uuid::new_v4();

    let stream1 = Stream {
        handle: StreamHandle {
            id: stream_id,
            peer: node2.clone(),
            stream_type: "bidirectional_test".to_string(),
            sender: StreamSender::new(tx1_to_2, flow_control1.clone()),
            receiver: StreamReceiver::new(rx2_to_1, flow_control1.clone()),
            flow_control: flow_control1,
        },
    };

    let stream2 = Stream {
        handle: StreamHandle {
            id: stream_id,
            peer: node1.clone(),
            stream_type: "bidirectional_test".to_string(),
            sender: StreamSender::new(tx2_to_1, flow_control2.clone()),
            receiver: StreamReceiver::new(rx1_to_2, flow_control2.clone()),
            flow_control: flow_control2,
        },
    };

    info!(
        "Testing bidirectional streaming between {} and {}",
        node1, node2
    );

    // Node 1 sends numbers 0-9
    let task1 = tokio::spawn(async move {
        for i in 0..10 {
            let msg = Bytes::from(format!("Node1 message {i}"));
            stream1.send(msg).await.expect("Failed to send from node1");

            // Also receive and process messages from node2
            if let Some(data) = stream1.recv().await {
                let msg = String::from_utf8_lossy(&data);
                info!("Node1 received: {}", msg);
            }
        }
    });

    // Node 2 sends letters A-J
    let task2 = tokio::spawn(async move {
        for i in 0..10 {
            let letter = char::from(b'A' + i as u8);
            let msg = Bytes::from(format!("Node2 message {letter}"));
            stream2.send(msg).await.expect("Failed to send from node2");

            // Also receive and process messages from node1
            if let Some(data) = stream2.recv().await {
                let msg = String::from_utf8_lossy(&data);
                info!("Node2 received: {}", msg);
            }
        }
    });

    // Wait for both tasks
    task1.await.unwrap();
    task2.await.unwrap();

    info!("Bidirectional streaming test completed successfully!");
}

#[tokio::test]
async fn test_streaming_with_errors() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    // Create a stream that will be abruptly closed
    let (tx, rx) = bounded::<Bytes>(10);
    let flow_control = Arc::new(FlowController::new(65536));

    let sender = StreamSender::new(tx, flow_control.clone());
    let receiver = StreamReceiver::new(rx, flow_control);

    // Send some messages
    for i in 0..5 {
        let msg = Bytes::from(format!("Message {i}"));
        sender.send(msg).await.expect("Failed to send");
    }

    // Drop the sender to close the stream
    drop(sender);
    info!("Sender dropped, stream should be closed");

    // Try to receive remaining messages
    let mut received = 0;
    while let Some(data) = receiver.recv().await {
        let msg = String::from_utf8_lossy(&data);
        info!("Received before close: {}", msg);
        received += 1;
    }

    assert_eq!(received, 5, "Should receive all sent messages");

    // Next receive should return None
    assert!(receiver.recv().await.is_none(), "Stream should be closed");

    info!("Error handling test completed successfully!");
}
