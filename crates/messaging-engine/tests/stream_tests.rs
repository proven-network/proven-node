//! Integration tests for engine-based streams

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use proven_engine::{EngineBuilder, EngineConfig, RetentionPolicy, StreamConfig};
use proven_messaging::stream::{InitializedStream, Stream};
use proven_messaging_engine::stream::{EngineStream, EngineStreamOptions};
use proven_network::NetworkManager;
use proven_storage_memory::MemoryStorage;
use proven_topology::{Node as TopologyNode, NodeId, TopologyManager};
use proven_topology_mock::MockTopologyAdaptor;
use proven_transport_memory::MemoryTransport;
use serde::{Deserialize, Serialize};
use tracing_test::traced_test;

/// Test message type
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct TestMessage {
    id: u64,
    content: String,
}

impl TryFrom<Bytes> for TestMessage {
    type Error = serde_cbor::Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        serde_cbor::from_slice(&bytes)
    }
}

impl TryInto<Bytes> for TestMessage {
    type Error = serde_cbor::Error;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let vec = serde_cbor::to_vec(&self)?;
        Ok(Bytes::from(vec))
    }
}

/// Helper to create a test engine client
async fn create_test_engine()
-> proven_engine::Client<MemoryTransport, MockTopologyAdaptor, MemoryStorage> {
    use std::sync::atomic::{AtomicU8, Ordering};
    static NODE_COUNTER: AtomicU8 = AtomicU8::new(1);

    let node_id = NodeId::from_seed(NODE_COUNTER.fetch_add(1, Ordering::SeqCst));

    // Create memory transport
    let transport = Arc::new(MemoryTransport::new(node_id.clone()));
    transport
        .register(&format!("memory://{node_id}"))
        .await
        .expect("Failed to register transport");

    // Create mock topology
    let node = TopologyNode::new(
        "test-az".to_string(),
        format!("memory://{node_id}"),
        node_id.clone(),
        "test-region".to_string(),
        Default::default(),
    );

    let topology =
        MockTopologyAdaptor::new(vec![], vec![], "https://auth.test.com".to_string(), vec![]);
    let _ = topology.add_node(node);
    let topology_manager = Arc::new(TopologyManager::new(Arc::new(topology), node_id.clone()));

    // Create network manager
    let network_manager = Arc::new(NetworkManager::new(
        node_id.clone(),
        transport,
        topology_manager.clone(),
    ));

    // Create storage
    let storage = MemoryStorage::new();

    // Configure engine
    let mut config = EngineConfig::default();
    config.consensus.global.election_timeout_min = Duration::from_millis(50);
    config.consensus.global.election_timeout_max = Duration::from_millis(100);
    config.consensus.global.heartbeat_interval = Duration::from_millis(20);

    // Build and start engine
    let mut engine = EngineBuilder::new(node_id.clone())
        .with_config(config)
        .with_network(network_manager)
        .with_topology(topology_manager)
        .with_storage(storage)
        .build()
        .await
        .expect("Failed to build engine");

    engine.start().await.expect("Failed to start engine");

    // Engine start() now ensures default group exists
    engine.client()
}

#[traced_test]
#[tokio::test]
async fn test_stream_creation_and_initialization() {
    let client = create_test_engine().await;

    // Create stream with options
    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestMessage, _, _> =
        EngineStream::new("test_stream", options);

    // Initialize the stream
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Verify stream name
    assert_eq!(initialized.name(), "test_stream");

    // Verify stream exists by checking messages count
    let messages = initialized
        .messages()
        .await
        .expect("Failed to get messages count");
    assert_eq!(messages, 0);
}

#[traced_test]
#[tokio::test]
async fn test_publish_and_retrieve_single_message() {
    let client = create_test_engine().await;

    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestMessage, _, _> =
        EngineStream::new("test_publish", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Create and publish a test message
    let message = TestMessage {
        id: 1,
        content: "Hello, Engine!".to_string(),
    };

    let seq = initialized
        .publish(message.clone())
        .await
        .expect("Failed to publish message");
    assert_eq!(seq, 1);

    // Retrieve the message
    let retrieved = initialized.get(seq).await.expect("Failed to get message");
    assert_eq!(retrieved, Some(message));

    // Verify last sequence
    let last_seq = initialized
        .last_seq()
        .await
        .expect("Failed to get last sequence");
    assert_eq!(last_seq, 1);

    // Verify message count
    let count = initialized
        .messages()
        .await
        .expect("Failed to get message count");
    assert_eq!(count, 1);
}

#[traced_test]
#[tokio::test]
async fn test_publish_batch() {
    let client = create_test_engine().await;

    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestMessage, _, _> = EngineStream::new("test_batch", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Create batch of messages
    let messages: Vec<TestMessage> = (1..=5)
        .map(|i| TestMessage {
            id: i,
            content: format!("Batch message {i}"),
        })
        .collect();

    let last_seq = initialized
        .publish_batch(messages.clone())
        .await
        .expect("Failed to publish batch");
    assert_eq!(last_seq, 5);

    // Verify all messages
    for (i, expected) in messages.iter().enumerate() {
        let seq = (i + 1) as u64;
        let retrieved = initialized.get(seq).await.expect("Failed to get message");
        assert_eq!(retrieved.as_ref(), Some(expected));
    }

    // Verify message count
    let count = initialized
        .messages()
        .await
        .expect("Failed to get message count");
    assert_eq!(count, 5);
}

#[traced_test]
#[tokio::test]
async fn test_delete_message() {
    let client = create_test_engine().await;

    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestMessage, _, _> =
        EngineStream::new("test_delete", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Publish a message
    let message = TestMessage {
        id: 1,
        content: "To be deleted".to_string(),
    };

    let seq = initialized
        .publish(message)
        .await
        .expect("Failed to publish message");

    // Verify message exists
    let retrieved = initialized.get(seq).await.expect("Failed to get message");
    assert!(retrieved.is_some());

    // Delete the message
    initialized
        .delete(seq)
        .await
        .expect("Failed to delete message");

    // Small delay to allow async event processing
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Verify message is deleted (returns None)
    let after_delete = initialized
        .get(seq)
        .await
        .expect("Failed to get message after delete");
    assert!(after_delete.is_none());
}

#[traced_test]
#[tokio::test]
async fn test_last_message() {
    let client = create_test_engine().await;

    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestMessage, _, _> = EngineStream::new("test_last", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Initially no last message
    let last = initialized
        .last_message()
        .await
        .expect("Failed to get last message");
    assert!(last.is_none());

    // Publish messages
    for i in 1..=3 {
        let message = TestMessage {
            id: i,
            content: format!("Message {i}"),
        };
        initialized
            .publish(message)
            .await
            .expect("Failed to publish message");
    }

    // Get last message
    let last = initialized
        .last_message()
        .await
        .expect("Failed to get last message");
    let expected = TestMessage {
        id: 3,
        content: "Message 3".to_string(),
    };
    assert_eq!(last, Some(expected));
}

#[traced_test]
#[tokio::test]
async fn test_rollup() {
    let client = create_test_engine().await;

    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestMessage, _, _> =
        EngineStream::new("test_rollup", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Publish some messages
    for i in 1..=5 {
        let message = TestMessage {
            id: i,
            content: format!("Pre-rollup {i}"),
        };
        initialized
            .publish(message)
            .await
            .expect("Failed to publish message");
    }

    // Create rollup message
    let rollup_message = TestMessage {
        id: 100,
        content: "Rollup of all previous messages".to_string(),
    };

    let rollup_seq = initialized
        .rollup(rollup_message.clone(), 5)
        .await
        .expect("Failed to rollup");
    assert_eq!(rollup_seq, 6);

    // Verify rollup message
    let retrieved = initialized
        .get(rollup_seq)
        .await
        .expect("Failed to get rollup message");
    assert_eq!(retrieved, Some(rollup_message));

    // Note: In a real implementation, previous messages might be removed after rollup
    // But the current implementation just adds the rollup message
}

#[traced_test]
#[tokio::test]
async fn test_stream_with_custom_config() {
    let client = create_test_engine().await;

    // Create stream with custom configuration
    let stream_config = StreamConfig {
        max_message_size: 2 * 1024 * 1024,                  // 2MB
        retention: RetentionPolicy::Time { seconds: 3600 }, // 1 hour
        ..Default::default()
    };

    let options = EngineStreamOptions::new(client.clone(), Some(stream_config));
    let stream: EngineStream<_, _, _, TestMessage, _, _> =
        EngineStream::new("test_config", options);

    // Initialize should succeed with custom config
    let initialized = stream
        .init()
        .await
        .expect("Failed to initialize stream with config");
    assert_eq!(initialized.name(), "test_config");
}

#[traced_test]
#[tokio::test]
async fn test_empty_batch_publish() {
    let client = create_test_engine().await;

    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestMessage, _, _> =
        EngineStream::new("test_empty_batch", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Publish empty batch
    let last_seq = initialized
        .publish_batch(vec![])
        .await
        .expect("Failed to publish empty batch");
    assert_eq!(last_seq, 0);

    // Verify no messages
    let count = initialized
        .messages()
        .await
        .expect("Failed to get message count");
    assert_eq!(count, 0);
}

#[traced_test]
#[tokio::test]
async fn test_concurrent_publish() {
    let client = create_test_engine().await;

    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestMessage, _, _> =
        EngineStream::new("test_concurrent", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Spawn multiple tasks to publish concurrently
    let mut handles = vec![];

    for i in 0..5 {
        let stream_clone = initialized.clone();
        let handle = tokio::spawn(async move {
            let message = TestMessage {
                id: i,
                content: format!("Concurrent message {i}"),
            };
            stream_clone
                .publish(message)
                .await
                .expect("Failed to publish concurrent message")
        });
        handles.push(handle);
    }

    // Wait for all publishes
    let mut sequences = vec![];
    for handle in handles {
        sequences.push(handle.await.expect("Task failed"));
    }

    // Verify all sequences are unique and sequential
    sequences.sort();
    for (i, &seq) in sequences.iter().enumerate() {
        assert_eq!(seq, (i + 1) as u64);
    }

    // Verify total count
    let count = initialized
        .messages()
        .await
        .expect("Failed to get message count");
    assert_eq!(count, 5);
}

#[traced_test]
#[tokio::test]
async fn test_get_nonexistent_message() {
    let client = create_test_engine().await;

    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestMessage, _, _> =
        EngineStream::new("test_nonexistent", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Try to get a message that doesn't exist
    let result = initialized.get(999).await;

    // The engine returns an error for non-existent sequences, but we want None
    match result {
        Ok(None) => {} // Expected case
        Ok(Some(_)) => panic!("Should not find message at sequence 999"),
        Err(e) => {
            // Engine returns error for messages beyond stream bounds, which is acceptable
            assert!(e.to_string().contains("Not found") || e.to_string().contains("out of bounds"));
        }
    }
}

#[traced_test]
#[tokio::test]
async fn test_stream_persistence() {
    let client = create_test_engine().await;

    // Create and use first stream instance
    {
        let options = EngineStreamOptions::new(client.clone(), None);
        let stream: EngineStream<_, _, _, TestMessage, _, _> =
            EngineStream::new("test_persist", options);
        let initialized = stream.init().await.expect("Failed to initialize stream");

        let message = TestMessage {
            id: 1,
            content: "Persistent message".to_string(),
        };

        initialized
            .publish(message)
            .await
            .expect("Failed to publish message");
    }

    // Create new stream instance with same name
    {
        let options = EngineStreamOptions::new(client.clone(), None);
        let stream: EngineStream<_, _, _, TestMessage, _, _> =
            EngineStream::new("test_persist", options);
        let initialized = stream.init().await.expect("Failed to initialize stream");

        // Should find the previously published message
        let retrieved = initialized.get(1).await.expect("Failed to get message");
        let expected = TestMessage {
            id: 1,
            content: "Persistent message".to_string(),
        };
        assert_eq!(retrieved, Some(expected));
    }
}

#[traced_test]
#[tokio::test]
async fn test_publish_with_metadata() {
    let client = create_test_engine().await;

    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestMessage, _, _> =
        EngineStream::new("test_metadata", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Create message and metadata
    let message = TestMessage {
        id: 1,
        content: "Message with metadata".to_string(),
    };

    let mut metadata = std::collections::HashMap::new();
    metadata.insert("source".to_string(), "test".to_string());
    metadata.insert("priority".to_string(), "high".to_string());

    // Convert to bytes for metadata publish
    let message_bytes: Bytes = message.try_into().expect("Failed to serialize message");

    let seq = initialized
        .publish_with_metadata(message_bytes, metadata)
        .await
        .expect("Failed to publish with metadata");
    assert_eq!(seq, 1);

    // Note: Current implementation doesn't store metadata separately,
    // so we can only verify the message was published
    let count = initialized
        .messages()
        .await
        .expect("Failed to get message count");
    assert_eq!(count, 1);
}
