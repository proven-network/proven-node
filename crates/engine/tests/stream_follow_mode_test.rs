//! Tests for stream follow mode functionality
//!
//! These tests verify that streams can operate in "follow mode" where they
//! continuously wait for new messages instead of ending when caught up.

use futures::StreamExt;
use proven_engine::{PersistenceType, RetentionPolicy, StreamConfig};
use std::num::NonZero;
use std::time::Duration;

mod common;
use common::test_cluster::{TestCluster, TransportType};

#[tokio::test]
async fn test_stream_follow_mode_basic() {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=info")
        .with_test_writer()
        .try_init();

    // Create a single-node test cluster
    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _node_infos) = cluster.add_nodes(1).await;

    // Wait for cluster formation
    tokio::time::sleep(Duration::from_secs(2)).await;

    let client = engines[0].client();

    // Create a stream
    let stream_name = "follow-test-stream".to_string();
    let stream_config = StreamConfig {
        persistence_type: PersistenceType::Persistent,
        retention: RetentionPolicy::Count { max_messages: 1000 },
        max_message_size: 1024 * 1024, // 1MB
        allow_auto_create: false,
    };

    client
        .create_stream(stream_name.clone(), stream_config)
        .await
        .expect("Failed to create stream");

    // Publish some initial messages
    let initial_messages: Vec<Vec<u8>> = (0..5)
        .map(|i| format!("message-{i}").into_bytes())
        .collect();

    for msg in &initial_messages {
        client
            .publish(stream_name.clone(), msg.clone(), None)
            .await
            .expect("Failed to publish message");
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    // Start a stream reader in follow mode
    let mut stream = client
        .stream_messages(stream_name.clone(), NonZero::new(1).unwrap(), None)
        .await
        .expect("Failed to create stream reader")
        .follow();

    // Read the initial messages
    for expected_message in initial_messages.iter().take(5) {
        let msg = stream
            .next()
            .await
            .expect("Stream ended unexpectedly")
            .expect("Failed to read message");
        assert_eq!(msg.data.payload.as_ref(), expected_message);
    }

    // Now test follow mode by appending a message after starting the reader
    let client_clone = client.clone();
    let stream_name_clone = stream_name.clone();

    // Spawn a task to publish a message after a delay
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        client_clone
            .publish(stream_name_clone, b"follow-message".to_vec(), None)
            .await
            .expect("Failed to publish follow message");
    });

    // Read the follow message (should wait for it)
    let result = tokio::time::timeout(Duration::from_secs(5), stream.next()).await;

    let msg = result
        .expect("Timeout waiting for follow message")
        .expect("Stream ended unexpectedly")
        .expect("Failed to read message");

    assert_eq!(msg.data.payload.as_ref(), b"follow-message");

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }
}

#[tokio::test]
async fn test_stream_follow_mode_with_backoff() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=info")
        .with_test_writer()
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _node_infos) = cluster.add_nodes(1).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let client = engines[0].client();

    // Create a stream
    let stream_name = "backoff-test-stream".to_string();
    let stream_config = StreamConfig {
        persistence_type: PersistenceType::Persistent,
        retention: RetentionPolicy::Count { max_messages: 1000 },
        max_message_size: 1024 * 1024, // 1MB
        allow_auto_create: false,
    };

    client
        .create_stream(stream_name.clone(), stream_config)
        .await
        .expect("Failed to create stream");

    // Start a stream reader in follow mode
    let mut stream = client
        .stream_messages(stream_name.clone(), NonZero::new(1).unwrap(), None)
        .await
        .expect("Failed to create stream reader")
        .follow();

    // Test that follow mode waits with exponential backoff
    let start = tokio::time::Instant::now();

    // Publish a message after 1 second
    let client_clone = client.clone();
    let stream_name_clone = stream_name.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        client_clone
            .publish(stream_name_clone, b"delayed-message".to_vec(), None)
            .await
            .expect("Failed to publish message");
    });

    // Read should wait for the message
    let msg = stream
        .next()
        .await
        .expect("Stream ended unexpectedly")
        .expect("Failed to read message");
    assert_eq!(msg.data.payload.as_ref(), b"delayed-message");

    // Verify timing
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(900),
        "Message arrived too early"
    );
    assert!(
        elapsed <= Duration::from_millis(2000),
        "Message took too long"
    );

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }
}

#[tokio::test]
async fn test_stream_follow_mode_cancellation() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("proven_engine=info")
        .with_test_writer()
        .try_init();

    let mut cluster = TestCluster::new(TransportType::Tcp);
    let (engines, _node_infos) = cluster.add_nodes(1).await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let client = engines[0].client();

    // Create a stream
    let stream_name = "cancel-test-stream".to_string();
    let stream_config = StreamConfig {
        persistence_type: PersistenceType::Persistent,
        retention: RetentionPolicy::Count { max_messages: 1000 },
        max_message_size: 1024 * 1024, // 1MB
        allow_auto_create: false,
    };

    client
        .create_stream(stream_name.clone(), stream_config)
        .await
        .expect("Failed to create stream");

    // Start a stream reader in follow mode
    let stream = client
        .stream_messages(stream_name.clone(), NonZero::new(1).unwrap(), None)
        .await
        .expect("Failed to create stream reader")
        .follow();

    // Drop the stream to test cleanup
    drop(stream);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify we can create a new stream reader and use it normally
    let mut new_stream = client
        .stream_messages(stream_name.clone(), NonZero::new(1).unwrap(), None)
        .await
        .expect("Failed to create new stream reader");

    // Publish and read a message
    client
        .publish(stream_name, b"test-message".to_vec(), None)
        .await
        .expect("Failed to publish message");

    let msg = new_stream
        .next()
        .await
        .expect("Stream ended unexpectedly")
        .expect("Failed to read message");
    assert_eq!(msg.data.payload.as_ref(), b"test-message");

    // Clean up
    for mut engine in engines {
        engine.stop().await.expect("Failed to stop engine");
    }
}
