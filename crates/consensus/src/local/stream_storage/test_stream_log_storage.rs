//! Tests for LogStorage implementation with stream storage

#[cfg(test)]
mod tests {
    use crate::{
        allocation::ConsensusGroupId,
        local::stream_storage::{
            log_types::{CompressionType, MessageSource, StreamMetadata as StreamLogMetadata},
            stream_manager::create_stream_manager,
            traits::{RetentionPolicy, StorageType, StreamConfig},
        },
        storage::{StorageEngine, StorageNamespace, log::LogEntry},
    };
    use bytes::Bytes;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_stream_log_storage_operations() {
        let group_id = ConsensusGroupId(1);
        let stream_manager = create_stream_manager(group_id, None).unwrap();

        // Create a stream with memory storage
        let config = StreamConfig {
            max_messages: Some(1000),
            max_bytes: None,
            max_age_secs: Some(3600),
            storage_type: StorageType::Memory,
            retention_policy: RetentionPolicy::Limits,
            pubsub_bridge_enabled: false,
            consensus_group: None,
            compact_on_deletion: false,
            compression: crate::local::stream_storage::traits::CompressionType::None,
        };

        let storage = stream_manager
            .create_stream("test_stream".to_string(), config)
            .await
            .unwrap();

        // Create namespace for messages
        let namespace = StorageNamespace::new("messages");
        storage
            .create_namespace(&namespace)
            .await
            .expect("Failed to create namespace");

        // Test append_entry
        let metadata1 = StreamLogMetadata {
            headers: HashMap::from([("key1".to_string(), "value1".to_string())]),
            compression: None,
            source: MessageSource::Consensus,
        };

        let entry1 = LogEntry {
            index: 1,
            timestamp: chrono::Utc::now().timestamp() as u64,
            data: Bytes::from("Message 1"),
            metadata: metadata1,
        };

        storage
            .append_stream_entry(&namespace, entry1)
            .await
            .expect("Failed to append entry");

        // Test append_entries (batch)
        let metadata2 = StreamLogMetadata {
            headers: HashMap::new(),
            compression: Some(CompressionType::Gzip),
            source: MessageSource::PubSub {
                subject: "test.subject".to_string(),
                publisher: "node123".to_string(),
            },
        };

        let entry2 = LogEntry {
            index: 2,
            timestamp: chrono::Utc::now().timestamp() as u64,
            data: Bytes::from("Message 2"),
            metadata: metadata2,
        };

        let metadata3 = StreamLogMetadata {
            headers: HashMap::new(),
            compression: None,
            source: MessageSource::Migration { source_group: 42 },
        };

        let entry3 = LogEntry {
            index: 3,
            timestamp: chrono::Utc::now().timestamp() as u64,
            data: Bytes::from("Message 3"),
            metadata: metadata3,
        };

        storage
            .append_stream_entries(&namespace, vec![entry2, entry3])
            .await
            .expect("Failed to append entries");

        // Test get_entry
        let retrieved: LogEntry<StreamLogMetadata> = storage
            .get_stream_entry(&namespace, 2)
            .await
            .expect("Failed to get entry")
            .expect("Entry not found");

        assert_eq!(retrieved.index, 2);
        assert_eq!(retrieved.data, Bytes::from("Message 2"));

        // Test get_last_entry
        let last: LogEntry<StreamLogMetadata> = storage
            .get_last_stream_entry(&namespace)
            .await
            .expect("Failed to get last entry")
            .expect("No last entry");

        assert_eq!(last.index, 3);
        assert_eq!(last.data, Bytes::from("Message 3"));

        // Test get_first_entry
        let first: LogEntry<StreamLogMetadata> = storage
            .get_first_stream_entry(&namespace)
            .await
            .expect("Failed to get first entry")
            .expect("No first entry");

        assert_eq!(first.index, 1);
        assert_eq!(first.data, Bytes::from("Message 1"));

        // Test read_range
        let range_entries: Vec<LogEntry<StreamLogMetadata>> = storage
            .read_stream_range(&namespace, 1..=2)
            .await
            .expect("Failed to read range");

        assert_eq!(range_entries.len(), 2);
        assert_eq!(range_entries[0].index, 1);
        assert_eq!(range_entries[1].index, 2);

        // Test get_log_state
        let state = storage
            .get_stream_log_state(&namespace)
            .await
            .expect("Failed to get log state")
            .expect("No log state");

        assert_eq!(state.first_index, 1);
        assert_eq!(state.last_index, 3);
        assert_eq!(state.entry_count, 3);

        // Test truncate (remove entry 3)
        storage
            .truncate_stream_log(&namespace, 2)
            .await
            .expect("Failed to truncate");

        // Verify truncation
        let after_truncate: Option<LogEntry<StreamLogMetadata>> = storage
            .get_stream_entry(&namespace, 3)
            .await
            .expect("Failed to check after truncate");
        assert!(after_truncate.is_none());

        // Verify entry 2 still exists
        let entry2_exists: Option<LogEntry<StreamLogMetadata>> = storage
            .get_stream_entry(&namespace, 2)
            .await
            .expect("Failed to check entry 2");
        assert!(entry2_exists.is_some());
    }

    #[tokio::test]
    async fn test_stream_metadata_variants() {
        // Test all metadata source variants can be serialized/deserialized
        let sources = vec![
            MessageSource::Consensus,
            MessageSource::PubSub {
                subject: "test.topic".to_string(),
                publisher: "publisher-id".to_string(),
            },
            MessageSource::Migration { source_group: 123 },
        ];

        for source in sources {
            let metadata = StreamLogMetadata {
                headers: HashMap::from([("test".to_string(), "value".to_string())]),
                compression: Some(CompressionType::Zstd),
                source: source.clone(),
            };

            // Test serialization
            let mut buffer = Vec::new();
            ciborium::into_writer(&metadata, &mut buffer).expect("Failed to serialize");

            let deserialized: StreamLogMetadata =
                ciborium::from_reader(&buffer[..]).expect("Failed to deserialize");

            // Verify source matches
            match (source, deserialized.source) {
                (MessageSource::Consensus, MessageSource::Consensus) => {}
                (
                    MessageSource::PubSub {
                        subject: s1,
                        publisher: p1,
                    },
                    MessageSource::PubSub {
                        subject: s2,
                        publisher: p2,
                    },
                ) => {
                    assert_eq!(s1, s2);
                    assert_eq!(p1, p2);
                }
                (
                    MessageSource::Migration { source_group: g1 },
                    MessageSource::Migration { source_group: g2 },
                ) => {
                    assert_eq!(g1, g2);
                }
                _ => panic!("Source mismatch"),
            }
        }
    }
}
