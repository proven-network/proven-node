//! Event bus implementation

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::{RwLock, broadcast, mpsc};
use tracing::{debug, error, warn};
use uuid::Uuid;

use super::filters::EventFilter;
use super::types::*;

/// Event bus for publishing and subscribing to events
pub struct EventBus {
    /// Broadcast channel for events
    sender: broadcast::Sender<EventEnvelope>,
    /// Subscriber registry
    subscribers: Arc<RwLock<HashMap<String, SubscriberInfo>>>,
    /// Event deduplication cache
    dedup_cache: Arc<RwLock<HashSet<EventId>>>,
    /// Configuration
    config: EventConfig,
}

/// Subscriber information
struct SubscriberInfo {
    /// Subscriber ID
    id: String,
    /// Event filter
    filter: EventFilter,
    /// Subscription time
    subscribed_at: EventTimestamp,
}

impl EventBus {
    /// Create a new event bus
    pub fn new(config: EventConfig) -> Self {
        let (sender, _) = broadcast::channel(config.bus_capacity);
        Self {
            sender,
            subscribers: Arc::new(RwLock::new(HashMap::new())),
            dedup_cache: Arc::new(RwLock::new(HashSet::new())),
            config,
        }
    }

    /// Publish an event
    pub async fn publish(&self, envelope: EventEnvelope) -> EventingResult<()> {
        // Check deduplication
        if self.config.enable_deduplication {
            let mut cache = self.dedup_cache.write().await;
            if !cache.insert(envelope.metadata.id) {
                debug!("Duplicate event {} ignored", envelope.metadata.id);
                return Ok(());
            }

            // Clean old entries if cache is too large
            if cache.len() > 10000 {
                cache.clear();
            }
        }

        // Publish event
        self.sender
            .send(envelope.clone())
            .map_err(|_| EventError::PublishFailed("No active subscribers".to_string()))?;

        debug!(
            "Published event {} of type {:?}",
            envelope.metadata.id, envelope.metadata.event_type
        );

        Ok(())
    }

    /// Subscribe to events
    pub async fn subscribe(
        &self,
        subscriber_id: String,
        filter: EventFilter,
    ) -> EventingResult<EventSubscriber> {
        let subscribers = self.subscribers.read().await;
        if subscribers.len() >= self.config.max_subscribers {
            return Err(EventError::SubscribeFailed(
                "Maximum subscribers reached".to_string(),
            ));
        }
        drop(subscribers);

        let receiver = self.sender.subscribe();

        let info = SubscriberInfo {
            id: subscriber_id.clone(),
            filter: filter.clone(),
            subscribed_at: SystemTime::now(),
        };

        self.subscribers
            .write()
            .await
            .insert(subscriber_id.clone(), info);

        debug!(
            "Subscriber {} registered with filter {:?}",
            subscriber_id, filter
        );

        Ok(EventSubscriber {
            id: subscriber_id,
            receiver,
            filter,
        })
    }

    /// Unsubscribe from events
    pub async fn unsubscribe(&self, subscriber_id: &str) -> EventingResult<()> {
        self.subscribers.write().await.remove(subscriber_id);
        debug!("Subscriber {} unregistered", subscriber_id);
        Ok(())
    }

    /// Get active subscriber count
    pub async fn subscriber_count(&self) -> usize {
        self.subscribers.read().await.len()
    }

    /// Get subscriber information
    pub async fn get_subscribers(&self) -> Vec<String> {
        self.subscribers.read().await.keys().cloned().collect()
    }

    /// Clean expired deduplication entries
    pub async fn clean_dedup_cache(&self) {
        if self.config.enable_deduplication {
            let mut cache = self.dedup_cache.write().await;
            cache.clear();
            debug!("Cleared deduplication cache");
        }
    }
}

/// Event subscriber handle
pub struct EventSubscriber {
    /// Subscriber ID
    pub id: String,
    /// Receiver for events
    receiver: broadcast::Receiver<EventEnvelope>,
    /// Event filter
    filter: EventFilter,
}

impl EventSubscriber {
    /// Receive the next event matching the filter
    pub async fn recv(&mut self) -> Option<EventEnvelope> {
        loop {
            match self.receiver.recv().await {
                Ok(envelope) => {
                    if self.filter.matches(&envelope) {
                        return Some(envelope);
                    }
                    // Event doesn't match filter, continue
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("Subscriber {} lagged by {} messages", self.id, n);
                    // Continue receiving
                }
                Err(broadcast::error::RecvError::Closed) => {
                    error!("Event bus closed");
                    return None;
                }
            }
        }
    }

    /// Try to receive without blocking
    pub fn try_recv(&mut self) -> Option<EventEnvelope> {
        loop {
            match self.receiver.try_recv() {
                Ok(envelope) => {
                    if self.filter.matches(&envelope) {
                        return Some(envelope);
                    }
                    // Event doesn't match filter, continue
                }
                Err(broadcast::error::TryRecvError::Empty) => {
                    return None;
                }
                Err(broadcast::error::TryRecvError::Lagged(n)) => {
                    warn!("Subscriber {} lagged by {} messages", self.id, n);
                    // Continue receiving
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    error!("Event bus closed");
                    return None;
                }
            }
        }
    }

    /// Get the subscriber ID
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Get the event filter
    pub fn filter(&self) -> &EventFilter {
        &self.filter
    }
}

/// Event publisher handle
#[derive(Clone)]
pub struct EventPublisher {
    /// Channel for publishing events
    sender: mpsc::Sender<EventEnvelope>,
}

impl EventPublisher {
    /// Create a new publisher
    pub fn new(sender: mpsc::Sender<EventEnvelope>) -> Self {
        Self { sender }
    }

    /// Publish an event
    pub async fn publish(&self, event: Event, source: String) -> EventingResult<()> {
        let envelope = EventEnvelope {
            metadata: EventMetadata {
                id: Uuid::new_v4(),
                timestamp: SystemTime::now(),
                event_type: event.event_type(),
                priority: event.default_priority(),
                source,
                correlation_id: None,
                tags: Vec::new(),
            },
            event,
        };

        self.sender
            .send(envelope)
            .await
            .map_err(|_| EventError::PublishFailed("Failed to send event".to_string()))?;

        Ok(())
    }

    /// Publish an event with custom metadata
    pub async fn publish_with_metadata(
        &self,
        event: Event,
        metadata: EventMetadata,
    ) -> EventingResult<()> {
        let envelope = EventEnvelope { metadata, event };

        self.sender
            .send(envelope)
            .await
            .map_err(|_| EventError::PublishFailed("Failed to send event".to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        foundation::types::ConsensusGroupId,
        stream::{StreamConfig, StreamName},
    };

    use super::*;

    #[tokio::test]
    async fn test_event_bus_pub_sub() {
        let config = EventConfig::default();
        let bus = EventBus::new(config);

        // Subscribe
        let mut subscriber = bus
            .subscribe("test".to_string(), EventFilter::All)
            .await
            .unwrap();

        // Publish event
        let event = Event::StreamCreated {
            name: StreamName::new("test-stream"),
            config: StreamConfig::default(),
            group_id: ConsensusGroupId::new(1),
        };

        let envelope = EventEnvelope {
            metadata: EventMetadata {
                id: Uuid::new_v4(),
                timestamp: SystemTime::now(),
                event_type: EventType::Stream,
                priority: EventPriority::Normal,
                source: "test".to_string(),
                correlation_id: None,
                tags: vec![],
            },
            event,
        };

        bus.publish(envelope.clone()).await.unwrap();

        // Receive event
        let received = subscriber.recv().await.unwrap();
        assert_eq!(received.metadata.id, envelope.metadata.id);
    }

    #[tokio::test]
    async fn test_event_filtering() {
        let config = EventConfig::default();
        let bus = EventBus::new(config);

        // Subscribe with filter
        let mut subscriber = bus
            .subscribe(
                "test".to_string(),
                EventFilter::ByType(vec![EventType::Stream]),
            )
            .await
            .unwrap();

        // Publish stream event
        let stream_event = Event::StreamCreated {
            name: StreamName::new("test-stream"),
            config: StreamConfig::default(),
            group_id: ConsensusGroupId::new(1),
        };

        let envelope = EventEnvelope {
            metadata: EventMetadata {
                id: Uuid::new_v4(),
                timestamp: SystemTime::now(),
                event_type: EventType::Stream,
                priority: EventPriority::Normal,
                source: "test".to_string(),
                correlation_id: None,
                tags: vec![],
            },
            event: stream_event,
        };

        bus.publish(envelope).await.unwrap();

        // Publish group event (should be filtered out)
        let group_event = Event::GroupCreated {
            group_id: ConsensusGroupId::new(1),
            members: vec![],
        };

        let envelope = EventEnvelope {
            metadata: EventMetadata {
                id: Uuid::new_v4(),
                timestamp: SystemTime::now(),
                event_type: EventType::Group,
                priority: EventPriority::Normal,
                source: "test".to_string(),
                correlation_id: None,
                tags: vec![],
            },
            event: group_event,
        };

        bus.publish(envelope).await.unwrap();

        // Should only receive stream event
        let received = subscriber.recv().await.unwrap();
        assert_eq!(received.metadata.event_type, EventType::Stream);

        // Should not receive group event (non-blocking check)
        assert!(subscriber.try_recv().is_none());
    }
}
