//! Integration tests for message consumers

use std::error::Error as StdError;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use proven_bootable::Bootable;
use proven_engine::{EngineBuilder, EngineConfig};
use proven_logger::info;
use proven_logger_macros::logged_test;
use proven_messaging::consumer::Consumer;
use proven_messaging::consumer_handler::ConsumerHandler;
use proven_messaging::stream::{InitializedStream, Stream};
use proven_messaging_engine::consumer::EngineMessagingConsumerOptions;
use proven_messaging_engine::stream::{EngineStream, EngineStreamOptions};
use proven_network::NetworkManager;
use proven_storage::manager::StorageManager;
use proven_storage_memory::MemoryStorage;
use proven_topology::{Node as TopologyNode, NodeId, TopologyManager};
use proven_topology_mock::MockTopologyAdaptor;
use proven_transport_memory::MemoryTransport;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio::time::{sleep, timeout};

/// Simple error type for test handlers
#[derive(Debug)]
struct TestError(String);

impl TestError {
    fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl StdError for TestError {}

/// Test event type
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct TestEvent {
    id: u64,
    event_type: String,
    data: String,
}

impl TryFrom<Bytes> for TestEvent {
    type Error = serde_cbor::Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        serde_cbor::from_slice(&bytes)
    }
}

impl TryInto<Bytes> for TestEvent {
    type Error = serde_cbor::Error;

    fn try_into(self) -> Result<Bytes, Self::Error> {
        let vec = serde_cbor::to_vec(&self)?;
        Ok(Bytes::from(vec))
    }
}

/// Test consumer handler that tracks events
#[derive(Clone, Debug)]
struct TestConsumerHandler {
    processed_count: Arc<AtomicUsize>,
    events: Arc<RwLock<Vec<TestEvent>>>,
    sequences: Arc<RwLock<Vec<u64>>>,
    process_delay: Option<Duration>,
}

impl TestConsumerHandler {
    fn new() -> Self {
        Self {
            processed_count: Arc::new(AtomicUsize::new(0)),
            events: Arc::new(RwLock::new(Vec::new())),
            sequences: Arc::new(RwLock::new(Vec::new())),
            process_delay: None,
        }
    }

    fn with_delay(delay: Duration) -> Self {
        Self {
            processed_count: Arc::new(AtomicUsize::new(0)),
            events: Arc::new(RwLock::new(Vec::new())),
            sequences: Arc::new(RwLock::new(Vec::new())),
            process_delay: Some(delay),
        }
    }

    async fn get_processed_count(&self) -> usize {
        self.processed_count.load(Ordering::SeqCst)
    }

    async fn get_events(&self) -> Vec<TestEvent> {
        self.events.read().await.clone()
    }

    async fn get_sequences(&self) -> Vec<u64> {
        self.sequences.read().await.clone()
    }
}

#[async_trait]
impl ConsumerHandler<TestEvent, serde_cbor::Error, serde_cbor::Error> for TestConsumerHandler {
    type Error = TestError;

    async fn handle(&self, event: TestEvent, seq: u64) -> Result<(), Self::Error> {
        info!("Processing event {} at sequence {}", event.id, seq);

        // Optional delay to simulate processing time
        if let Some(delay) = self.process_delay {
            sleep(delay).await;
        }

        self.processed_count.fetch_add(1, Ordering::SeqCst);
        self.events.write().await.push(event);
        self.sequences.write().await.push(seq);

        Ok(())
    }

    async fn on_caught_up(&self) -> Result<(), Self::Error> {
        info!("Consumer caught up");
        Ok(())
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
    let storage_manager = Arc::new(StorageManager::new(MemoryStorage::new()));

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
        .with_storage(storage_manager)
        .build()
        .await
        .expect("Failed to build engine");

    engine.start().await.expect("Failed to start engine");

    // Wait for engine to initialize
    sleep(Duration::from_secs(5)).await;

    engine.client()
}

#[logged_test]
#[tokio::test]
async fn test_basic_consumer_processing() {
    let client = create_test_engine().await;

    // Create stream
    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestEvent, _, _> =
        EngineStream::new("test_consumer", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Publish some events
    let events = vec![
        TestEvent {
            id: 1,
            event_type: "created".to_string(),
            data: "First event".to_string(),
        },
        TestEvent {
            id: 2,
            event_type: "updated".to_string(),
            data: "Second event".to_string(),
        },
        TestEvent {
            id: 3,
            event_type: "deleted".to_string(),
            data: "Third event".to_string(),
        },
    ];

    for event in &events {
        initialized
            .publish(event.clone())
            .await
            .expect("Failed to publish event");
    }

    // Create consumer
    let handler = TestConsumerHandler::new();
    let consumer = initialized
        .consumer(
            "test_consumer",
            EngineMessagingConsumerOptions::default(),
            handler.clone(),
        )
        .await
        .expect("Failed to create consumer");

    // Start consumer
    consumer.start().await.expect("Failed to start consumer");

    // Wait for processing
    timeout(Duration::from_secs(5), async {
        while handler.get_processed_count().await < 3 {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Consumer didn't process all events");

    // Verify processed events
    let processed_events = handler.get_events().await;
    assert_eq!(processed_events, events);

    // Verify sequences
    let sequences = handler.get_sequences().await;
    assert_eq!(sequences, vec![1, 2, 3]);

    // Shutdown consumer
    consumer
        .shutdown()
        .await
        .expect("Failed to shutdown consumer");
}

#[logged_test]
#[tokio::test]
async fn test_consumer_starting_from_sequence() {
    let client = create_test_engine().await;

    // Create stream
    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestEvent, _, _> =
        EngineStream::new("test_from_seq", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Publish 5 events
    for i in 1..=5 {
        let event = TestEvent {
            id: i,
            event_type: "test".to_string(),
            data: format!("Event {i}"),
        };
        initialized
            .publish(event)
            .await
            .expect("Failed to publish event");
    }

    // Create consumer starting from sequence 3
    let handler = TestConsumerHandler::new();
    let consumer = initialized
        .consumer(
            "from_seq_consumer",
            EngineMessagingConsumerOptions {
                start_sequence: Some(3),
            },
            handler.clone(),
        )
        .await
        .expect("Failed to create consumer");

    // Start consumer
    consumer.start().await.expect("Failed to start consumer");

    // Wait for processing
    timeout(Duration::from_secs(5), async {
        while handler.get_processed_count().await < 3 {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Consumer didn't process expected events");

    // Should have processed events 3, 4, 5
    let processed_events = handler.get_events().await;
    assert_eq!(processed_events.len(), 3);
    assert_eq!(processed_events[0].id, 3);
    assert_eq!(processed_events[1].id, 4);
    assert_eq!(processed_events[2].id, 5);

    // Verify sequences
    let sequences = handler.get_sequences().await;
    assert_eq!(sequences, vec![3, 4, 5]);

    consumer
        .shutdown()
        .await
        .expect("Failed to shutdown consumer");
}

#[logged_test]
#[tokio::test]
async fn test_consumer_catches_up_with_live_stream() {
    let client = create_test_engine().await;

    // Create stream
    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestEvent, _, _> = EngineStream::new("test_catchup", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Publish initial events
    for i in 1..=3 {
        let event = TestEvent {
            id: i,
            event_type: "initial".to_string(),
            data: format!("Initial event {i}"),
        };
        initialized
            .publish(event)
            .await
            .expect("Failed to publish event");
    }

    // Create and start consumer
    let handler = TestConsumerHandler::new();
    let consumer = initialized
        .consumer(
            "catchup_consumer",
            EngineMessagingConsumerOptions::default(),
            handler.clone(),
        )
        .await
        .expect("Failed to create consumer");

    consumer.start().await.expect("Failed to start consumer");

    // Wait for initial processing
    timeout(Duration::from_secs(5), async {
        while handler.get_processed_count().await < 3 {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Consumer didn't process initial events");

    // Now publish more events while consumer is running
    for i in 4..=6 {
        let event = TestEvent {
            id: i,
            event_type: "live".to_string(),
            data: format!("Live event {i}"),
        };
        initialized
            .publish(event)
            .await
            .expect("Failed to publish event");
        sleep(Duration::from_millis(100)).await; // Small delay between publishes
    }

    // Wait for all events to be processed
    timeout(Duration::from_secs(5), async {
        while handler.get_processed_count().await < 6 {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Consumer didn't process all events");

    // Verify all events processed
    let processed_events = handler.get_events().await;
    assert_eq!(processed_events.len(), 6);

    consumer
        .shutdown()
        .await
        .expect("Failed to shutdown consumer");
}

#[logged_test]
#[tokio::test]
async fn test_multiple_consumers_same_stream() {
    let client = create_test_engine().await;

    // Create stream
    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestEvent, _, _> =
        EngineStream::new("test_multi_consumer", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Create multiple consumers
    let handler1 = TestConsumerHandler::new();
    let consumer1 = initialized
        .consumer(
            "consumer1",
            EngineMessagingConsumerOptions::default(),
            handler1.clone(),
        )
        .await
        .expect("Failed to create consumer1");

    let handler2 = TestConsumerHandler::new();
    let consumer2 = initialized
        .consumer(
            "consumer2",
            EngineMessagingConsumerOptions::default(),
            handler2.clone(),
        )
        .await
        .expect("Failed to create consumer2");

    // Start both consumers
    consumer1.start().await.expect("Failed to start consumer1");
    consumer2.start().await.expect("Failed to start consumer2");

    // Publish events
    for i in 1..=5 {
        let event = TestEvent {
            id: i,
            event_type: "shared".to_string(),
            data: format!("Shared event {i}"),
        };
        initialized
            .publish(event)
            .await
            .expect("Failed to publish event");
    }

    // Wait for both consumers to process all events
    timeout(Duration::from_secs(5), async {
        while handler1.get_processed_count().await < 5 || handler2.get_processed_count().await < 5 {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Consumers didn't process all events");

    // Verify both consumers processed all events
    let events1 = handler1.get_events().await;
    let events2 = handler2.get_events().await;
    assert_eq!(events1.len(), 5);
    assert_eq!(events2.len(), 5);
    assert_eq!(events1, events2); // Both should have same events

    consumer1
        .shutdown()
        .await
        .expect("Failed to shutdown consumer1");
    consumer2
        .shutdown()
        .await
        .expect("Failed to shutdown consumer2");
}

#[logged_test]
#[tokio::test]
async fn test_consumer_error_recovery() {
    let client = create_test_engine().await;

    // Create stream
    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestEvent, _, _> =
        EngineStream::new("test_error_recovery", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Error handler that fails on specific events
    #[derive(Clone, Debug)]
    struct ErrorConsumerHandler {
        processed_count: Arc<AtomicUsize>,
        fail_on_id: u64,
    }

    #[async_trait]
    impl ConsumerHandler<TestEvent, serde_cbor::Error, serde_cbor::Error> for ErrorConsumerHandler {
        type Error = TestError;

        async fn handle(&self, event: TestEvent, _seq: u64) -> Result<(), Self::Error> {
            if event.id == self.fail_on_id {
                Err(TestError::new(format!(
                    "Failed to process event {}",
                    event.id
                )))
            } else {
                self.processed_count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        async fn on_caught_up(&self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    // Create consumer that fails on event 3
    let handler = ErrorConsumerHandler {
        processed_count: Arc::new(AtomicUsize::new(0)),
        fail_on_id: 3,
    };

    let consumer = initialized
        .consumer(
            "error_consumer",
            EngineMessagingConsumerOptions::default(),
            handler.clone(),
        )
        .await
        .expect("Failed to create consumer");

    consumer.start().await.expect("Failed to start consumer");

    // Publish events
    for i in 1..=5 {
        let event = TestEvent {
            id: i,
            event_type: "test".to_string(),
            data: format!("Event {i}"),
        };
        initialized
            .publish(event)
            .await
            .expect("Failed to publish event");
    }

    // Wait a bit
    sleep(Duration::from_secs(2)).await;

    // Should have processed events 1, 2, 4, 5 (skipping 3)
    let count = handler.processed_count.load(Ordering::SeqCst);
    assert_eq!(count, 4); // All except the one that failed

    consumer
        .shutdown()
        .await
        .expect("Failed to shutdown consumer");
}

#[logged_test]
#[tokio::test]
async fn test_consumer_shutdown_and_restart() {
    let client = create_test_engine().await;

    // Create stream
    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestEvent, _, _> = EngineStream::new("test_restart", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Publish initial events
    for i in 1..=3 {
        let event = TestEvent {
            id: i,
            event_type: "initial".to_string(),
            data: format!("Event {i}"),
        };
        initialized
            .publish(event)
            .await
            .expect("Failed to publish event");
    }

    // Create and start first consumer
    let handler1 = TestConsumerHandler::new();
    let consumer1 = initialized
        .consumer(
            "restart_consumer",
            EngineMessagingConsumerOptions::default(),
            handler1.clone(),
        )
        .await
        .expect("Failed to create consumer");

    consumer1.start().await.expect("Failed to start consumer");

    // Wait for processing
    timeout(Duration::from_secs(5), async {
        while handler1.get_processed_count().await < 3 {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Consumer didn't process initial events");

    // Get last processed sequence
    let last_seq = consumer1
        .last_seq()
        .await
        .expect("Failed to get last sequence");
    assert_eq!(last_seq, 3);

    // Shutdown consumer
    consumer1
        .shutdown()
        .await
        .expect("Failed to shutdown consumer");
    consumer1.wait().await;

    // Publish more events while consumer is stopped
    for i in 4..=6 {
        let event = TestEvent {
            id: i,
            event_type: "while_stopped".to_string(),
            data: format!("Event {i}"),
        };
        initialized
            .publish(event)
            .await
            .expect("Failed to publish event");
    }

    // Create new consumer with same name, starting from where it left off
    let handler2 = TestConsumerHandler::new();
    let consumer2 = initialized
        .consumer(
            "restart_consumer",
            EngineMessagingConsumerOptions {
                start_sequence: Some(last_seq + 1),
            },
            handler2.clone(),
        )
        .await
        .expect("Failed to create consumer");

    consumer2.start().await.expect("Failed to start consumer");

    // Wait for new events to be processed
    timeout(Duration::from_secs(5), async {
        while handler2.get_processed_count().await < 3 {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Consumer didn't process new events");

    // Should have only processed events 4, 5, 6
    let processed_events = handler2.get_events().await;
    assert_eq!(processed_events.len(), 3);
    assert_eq!(processed_events[0].id, 4);
    assert_eq!(processed_events[1].id, 5);
    assert_eq!(processed_events[2].id, 6);

    consumer2
        .shutdown()
        .await
        .expect("Failed to shutdown consumer");
}

#[logged_test]
#[tokio::test]
async fn test_slow_consumer() {
    let client = create_test_engine().await;

    // Create stream
    let options = EngineStreamOptions::new(client.clone(), None);
    let stream: EngineStream<_, _, _, TestEvent, _, _> = EngineStream::new("test_slow", options);
    let initialized = stream.init().await.expect("Failed to initialize stream");

    // Create slow consumer (100ms delay per message)
    let handler = TestConsumerHandler::with_delay(Duration::from_millis(100));
    let consumer = initialized
        .consumer(
            "slow_consumer",
            EngineMessagingConsumerOptions::default(),
            handler.clone(),
        )
        .await
        .expect("Failed to create consumer");

    consumer.start().await.expect("Failed to start consumer");

    // Publish events rapidly
    for i in 1..=5 {
        let event = TestEvent {
            id: i,
            event_type: "rapid".to_string(),
            data: format!("Rapid event {i}"),
        };
        initialized
            .publish(event)
            .await
            .expect("Failed to publish event");
    }

    // Consumer should still process all events, just slowly
    timeout(Duration::from_secs(10), async {
        while handler.get_processed_count().await < 5 {
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Slow consumer didn't process all events");

    // Verify all events processed in order
    let sequences = handler.get_sequences().await;
    assert_eq!(sequences, vec![1, 2, 3, 4, 5]);

    consumer
        .shutdown()
        .await
        .expect("Failed to shutdown consumer");
}
