//! Basic streaming test using the stream primitives directly
//!
//! This test demonstrates the basic streaming functionality without
//! requiring full NetworkManager integration.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use flume::bounded;
use proven_network::stream::{
    FlowController, Stream, StreamConfig, StreamHandle, StreamReceiver, StreamSender,
};
use proven_topology::NodeId;
use tracing::info;
use uuid::Uuid;

#[tokio::test]
async fn test_basic_stream_communication() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    // Create mock node IDs
    let node1_id = NodeId::from_seed(1);
    let node2_id = NodeId::from_seed(2);

    // Create a bidirectional stream using flume channels
    let (tx1, rx1) = bounded::<Bytes>(100);
    let (tx2, rx2) = bounded::<Bytes>(100);

    // Create flow controllers
    let flow_control1 = Arc::new(FlowController::new(StreamConfig::default().initial_window));
    let flow_control2 = Arc::new(FlowController::new(StreamConfig::default().initial_window));

    // Create stream handles
    let stream_id = Uuid::new_v4();

    let stream1 = Stream {
        handle: StreamHandle {
            id: stream_id,
            peer: node2_id,
            stream_type: "test_stream".to_string(),
            sender: StreamSender::new(tx1, flow_control1.clone()),
            receiver: StreamReceiver::new(rx2, flow_control1.clone()),
            flow_control: flow_control1,
        },
    };

    let stream2 = Stream {
        handle: StreamHandle {
            id: stream_id,
            peer: node1_id,
            stream_type: "test_stream".to_string(),
            sender: StreamSender::new(tx2, flow_control2.clone()),
            receiver: StreamReceiver::new(rx1, flow_control2.clone()),
            flow_control: flow_control2,
        },
    };

    info!(
        "Created bidirectional stream between {} and {}",
        node1_id, node2_id
    );

    // Test 1: Simple message exchange
    info!("Test 1: Simple message exchange");

    // Send from stream1 to stream2
    let msg1 = Bytes::from("Hello from stream1");
    stream1.send(msg1.clone()).await.expect("Failed to send");

    // Receive on stream2
    let received = stream2.recv().await.expect("Failed to receive");
    assert_eq!(received, msg1);
    info!("Stream2 received: {:?}", String::from_utf8_lossy(&received));

    // Send response from stream2 to stream1
    let msg2 = Bytes::from("Hello back from stream2");
    stream2.send(msg2.clone()).await.expect("Failed to send");

    // Receive on stream1
    let received = stream1.recv().await.expect("Failed to receive");
    assert_eq!(received, msg2);
    info!("Stream1 received: {:?}", String::from_utf8_lossy(&received));

    // Test 2: Multiple messages
    info!("Test 2: Sending multiple messages");

    let send_task = tokio::spawn(async move {
        for i in 0..5 {
            let msg = Bytes::from(format!("Message {i}"));
            stream1.send(msg).await.expect("Failed to send");
            info!("Sent message {}", i);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    let recv_task = tokio::spawn(async move {
        let mut received_count = 0;
        while let Some(data) = stream2.recv().await {
            let msg = String::from_utf8_lossy(&data);
            info!("Received: {}", msg);
            received_count += 1;
            if received_count >= 5 {
                break;
            }
        }
        assert_eq!(received_count, 5);
    });

    send_task.await.unwrap();
    recv_task.await.unwrap();

    info!("Basic stream test completed successfully");
}

#[tokio::test]
async fn test_stream_flow_control() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    // Create a stream with small window size for testing flow control
    let initial_window = 100; // 100 bytes
    let (tx, rx) = bounded::<Bytes>(10);

    let flow_control = Arc::new(FlowController::new(initial_window));
    let sender = StreamSender::new(tx, flow_control.clone());
    let _receiver = StreamReceiver::new(rx, flow_control.clone());

    info!("Testing flow control with window size: {}", initial_window);

    // Test 1: Send within window
    let small_msg = Bytes::from("Hello"); // 5 bytes
    sender
        .send(small_msg.clone())
        .await
        .expect("Should send small message");
    info!("Sent small message (5 bytes)");

    // Test 2: Try to send large message that exceeds window
    let large_msg = Bytes::from(vec![0u8; 150]); // 150 bytes > 100 byte window

    let send_handle = tokio::spawn(async move {
        info!("Attempting to send large message (150 bytes)");
        sender.send(large_msg).await
    });

    // Give some time for the send to block
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The send should be blocked, waiting for window update
    assert!(!send_handle.is_finished(), "Large send should be blocked");

    // Simulate window update
    flow_control.update_send_window(100);
    info!("Updated send window by 100 bytes");

    // Now the send should complete
    let result = tokio::time::timeout(Duration::from_secs(1), send_handle).await;
    assert!(result.is_ok(), "Send should complete after window update");

    info!("Flow control test completed successfully");
}

#[tokio::test]
async fn test_stream_with_backpressure() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    // Create a bounded channel with small capacity to test backpressure
    let (tx, rx) = bounded::<Bytes>(3); // Only 3 messages can be buffered

    let flow_control = Arc::new(FlowController::new(1000)); // Large window to isolate channel backpressure
    let sender = StreamSender::new(tx, flow_control.clone());
    let receiver = StreamReceiver::new(rx, flow_control);

    info!("Testing backpressure with channel capacity: 3");

    // Spawn a slow consumer
    let consumer = tokio::spawn(async move {
        let mut count = 0;
        while let Some(data) = receiver.recv().await {
            count += 1;
            let msg = String::from_utf8_lossy(&data);
            info!("Consumer received message {}: {}", count, msg);

            // Simulate slow processing
            tokio::time::sleep(Duration::from_millis(100)).await;

            if count >= 10 {
                break;
            }
        }
        count
    });

    // Producer tries to send messages quickly
    let producer = tokio::spawn(async move {
        for i in 0..10 {
            let msg = Bytes::from(format!("Message {i}"));
            let start = std::time::Instant::now();

            sender.send(msg).await.expect("Failed to send");

            let elapsed = start.elapsed();
            info!("Producer sent message {} (took {:?})", i, elapsed);

            // First few messages should be fast, then slow due to backpressure
            if i > 3 {
                assert!(
                    elapsed.as_millis() > 50,
                    "Send should be slow due to backpressure"
                );
            }
        }
    });

    producer.await.unwrap();
    let received_count = consumer.await.unwrap();
    assert_eq!(received_count, 10);

    info!("Backpressure test completed successfully");
}

#[test]
fn test_stream_frame_construction() {
    use proven_network::stream::StreamFrame;

    // Test creating different frame types
    let stream_id = Uuid::new_v4();

    let open_frame = StreamFrame::Open {
        stream_id,
        stream_type: "test".to_string(),
        metadata: [("key".to_string(), "value".to_string())].into(),
    };

    let data_frame = StreamFrame::Data {
        stream_id,
        sequence: 42,
        payload: Bytes::from("test data"),
        fin: false,
    };

    let window_frame = StreamFrame::WindowUpdate {
        stream_id,
        increment: 1024,
    };

    let reset_frame = StreamFrame::Reset {
        stream_id,
        error_code: 1,
        reason: "test error".to_string(),
    };

    // Verify we can match on the frames
    match open_frame {
        StreamFrame::Open { stream_type, .. } => {
            assert_eq!(stream_type, "test");
        }
        _ => panic!("Wrong frame type"),
    }

    match data_frame {
        StreamFrame::Data {
            sequence, payload, ..
        } => {
            assert_eq!(sequence, 42);
            assert_eq!(payload, Bytes::from("test data"));
        }
        _ => panic!("Wrong frame type"),
    }

    match window_frame {
        StreamFrame::WindowUpdate { increment, .. } => {
            assert_eq!(increment, 1024);
        }
        _ => panic!("Wrong frame type"),
    }

    match reset_frame {
        StreamFrame::Reset {
            error_code, reason, ..
        } => {
            assert_eq!(error_code, 1);
            assert_eq!(reason, "test error");
        }
        _ => panic!("Wrong frame type"),
    }

    println!("Frame construction test completed successfully");
}
