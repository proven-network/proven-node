//! Event store for persistence

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::sync::RwLock;
use tracing::debug;

use super::types::*;

/// Query for retrieving events
#[derive(Debug, Clone)]
pub struct EventQuery {
    /// Filter by event type
    pub event_type: Option<EventType>,
    /// Filter by source
    pub source: Option<String>,
    /// Filter by time range
    pub time_range: Option<(EventTimestamp, EventTimestamp)>,
    /// Filter by correlation ID
    pub correlation_id: Option<EventId>,
    /// Maximum results
    pub limit: Option<usize>,
    /// Skip first N results
    pub offset: Option<usize>,
}

impl Default for EventQuery {
    fn default() -> Self {
        Self {
            event_type: None,
            source: None,
            time_range: None,
            correlation_id: None,
            limit: Some(100),
            offset: None,
        }
    }
}

/// Event history entry
#[derive(Debug, Clone)]
pub struct EventHistory {
    /// Event envelope
    pub envelope: EventEnvelope,
    /// Processing result
    pub result: Option<EventResult>,
    /// Processing duration
    pub processing_time: Option<Duration>,
}

/// Event store for persistence
pub struct EventStore {
    /// Stored events
    events: Arc<RwLock<VecDeque<EventHistory>>>,
    /// Maximum events to store
    max_events: usize,
    /// Retention duration
    retention_duration: Duration,
}

impl EventStore {
    /// Create a new event store
    pub fn new(max_events: usize, retention_duration: Duration) -> Self {
        Self {
            events: Arc::new(RwLock::new(VecDeque::new())),
            max_events,
            retention_duration,
        }
    }

    /// Store an event
    pub async fn store(
        &self,
        envelope: EventEnvelope,
        result: Option<EventResult>,
    ) -> EventingResult<()> {
        let mut events = self.events.write().await;

        // Check capacity
        if events.len() >= self.max_events {
            events.pop_front();
        }

        let history = EventHistory {
            envelope,
            result,
            processing_time: None,
        };

        events.push_back(history);

        Ok(())
    }

    /// Store an event with processing time
    pub async fn store_with_timing(
        &self,
        envelope: EventEnvelope,
        result: EventResult,
        processing_time: Duration,
    ) -> EventingResult<()> {
        let mut events = self.events.write().await;

        // Check capacity
        if events.len() >= self.max_events {
            events.pop_front();
        }

        let history = EventHistory {
            envelope,
            result: Some(result),
            processing_time: Some(processing_time),
        };

        events.push_back(history);

        Ok(())
    }

    /// Query events
    pub async fn query(&self, query: &EventQuery) -> EventingResult<Vec<EventHistory>> {
        let events = self.events.read().await;

        let mut results: Vec<_> = events
            .iter()
            .filter(|h| self.matches_query(h, query))
            .cloned()
            .collect();

        // Apply offset
        if let Some(offset) = query.offset {
            results = results.into_iter().skip(offset).collect();
        }

        // Apply limit
        if let Some(limit) = query.limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    /// Get event by ID
    pub async fn get_by_id(&self, event_id: &EventId) -> EventingResult<Option<EventHistory>> {
        let events = self.events.read().await;

        Ok(events
            .iter()
            .find(|h| h.envelope.metadata.id == *event_id)
            .cloned())
    }

    /// Get events by correlation ID
    pub async fn get_by_correlation_id(
        &self,
        correlation_id: &EventId,
    ) -> EventingResult<Vec<EventHistory>> {
        let events = self.events.read().await;

        Ok(events
            .iter()
            .filter(|h| h.envelope.metadata.correlation_id.as_ref() == Some(correlation_id))
            .cloned()
            .collect())
    }

    /// Clean expired events
    pub async fn clean_expired(&self) -> EventingResult<usize> {
        let mut events = self.events.write().await;
        let now = SystemTime::now();
        let before_count = events.len();

        events.retain(|h| {
            now.duration_since(h.envelope.metadata.timestamp)
                .unwrap_or(Duration::from_secs(0))
                < self.retention_duration
        });

        let removed = before_count - events.len();
        if removed > 0 {
            debug!("Cleaned {} expired events", removed);
        }

        Ok(removed)
    }

    /// Get store statistics
    pub async fn get_stats(&self) -> EventingResult<EventStoreStats> {
        let events = self.events.read().await;

        let mut type_counts = std::collections::HashMap::new();
        let mut total_processing_time = Duration::from_secs(0);
        let mut processed_count = 0;

        for history in events.iter() {
            *type_counts
                .entry(history.envelope.metadata.event_type)
                .or_insert(0) += 1;

            if let Some(time) = history.processing_time {
                total_processing_time += time;
                processed_count += 1;
            }
        }

        let avg_processing_time = if processed_count > 0 {
            total_processing_time / processed_count as u32
        } else {
            Duration::from_secs(0)
        };

        Ok(EventStoreStats {
            total_events: events.len(),
            type_counts,
            avg_processing_time,
            oldest_event: events.front().map(|h| h.envelope.metadata.timestamp),
            newest_event: events.back().map(|h| h.envelope.metadata.timestamp),
        })
    }

    /// Check if event matches query
    fn matches_query(&self, history: &EventHistory, query: &EventQuery) -> bool {
        // Check event type
        if let Some(event_type) = &query.event_type
            && history.envelope.metadata.event_type != *event_type
        {
            return false;
        }

        // Check source
        if let Some(source) = &query.source
            && history.envelope.metadata.source != *source
        {
            return false;
        }

        // Check time range
        if let Some((start, end)) = &query.time_range {
            let timestamp = history.envelope.metadata.timestamp;
            if timestamp < *start || timestamp > *end {
                return false;
            }
        }

        // Check correlation ID
        if let Some(correlation_id) = &query.correlation_id
            && history.envelope.metadata.correlation_id.as_ref() != Some(correlation_id)
        {
            return false;
        }

        true
    }
}

/// Event store statistics
#[derive(Debug, Clone)]
pub struct EventStoreStats {
    /// Total number of events
    pub total_events: usize,
    /// Events by type
    pub type_counts: std::collections::HashMap<EventType, usize>,
    /// Average processing time
    pub avg_processing_time: Duration,
    /// Oldest event timestamp
    pub oldest_event: Option<EventTimestamp>,
    /// Newest event timestamp
    pub newest_event: Option<EventTimestamp>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        foundation::types::ConsensusGroupId,
        services::stream::{StreamConfig, StreamName},
    };
    use uuid::Uuid;

    #[tokio::test]
    async fn test_event_store() {
        let store = EventStore::new(100, Duration::from_secs(3600));

        // Store event
        let envelope = EventEnvelope {
            metadata: EventMetadata {
                id: Uuid::new_v4(),
                timestamp: SystemTime::now(),
                event_type: EventType::Stream,
                priority: EventPriority::Normal,
                source: "test".to_string(),
                correlation_id: None,
                tags: vec![],
                synchronous: false,
            },
            event: Event::StreamCreated {
                name: StreamName::new("test-stream"),
                config: StreamConfig::default(),
                group_id: ConsensusGroupId::new(1),
            },
        };

        store
            .store(envelope.clone(), Some(EventResult::Success))
            .await
            .unwrap();

        // Query events
        let query = EventQuery {
            event_type: Some(EventType::Stream),
            ..Default::default()
        };

        let results = store.query(&query).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].envelope.metadata.id, envelope.metadata.id);
    }

    #[tokio::test]
    async fn test_event_store_capacity() {
        let store = EventStore::new(2, Duration::from_secs(3600));

        // Store 3 events (exceeds capacity)
        for i in 0..3 {
            let envelope = EventEnvelope {
                metadata: EventMetadata {
                    id: Uuid::new_v4(),
                    timestamp: SystemTime::now(),
                    event_type: EventType::Stream,
                    priority: EventPriority::Normal,
                    source: format!("test-{i}"),
                    correlation_id: None,
                    tags: vec![],
                    synchronous: false,
                },
                event: Event::Custom {
                    event_type: "test".to_string(),
                    payload: serde_json::Value::Null,
                },
            };

            store.store(envelope, None).await.unwrap();
        }

        // Should only have 2 events (oldest was removed)
        let stats = store.get_stats().await.unwrap();
        assert_eq!(stats.total_events, 2);

        // First event should be from source "test-1"
        let query = EventQuery::default();
        let results = store.query(&query).await.unwrap();
        assert_eq!(results[0].envelope.metadata.source, "test-1");
    }
}
