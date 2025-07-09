//! Tests for LogStorage implementation in local group storage

#[cfg(test)]
mod tests {
    use crate::{
        allocation::ConsensusGroupId,
        local::group_storage::log_types::{LocalEntryType, LocalLogMetadata, StreamOperationType},
        node_id::NodeId,
    };

    #[tokio::test]
    async fn test_log_metadata_serialization() {
        let metadata = LocalLogMetadata {
            term: 5,
            leader_node_id: NodeId::from_seed(1),
            entry_type: LocalEntryType::StreamOperation {
                stream_id: "test-stream".to_string(),
                operation: StreamOperationType::Publish,
            },
            group_id: ConsensusGroupId(42),
        };

        // Test serialization/deserialization
        let mut buffer = Vec::new();
        ciborium::into_writer(&metadata, &mut buffer).expect("Failed to serialize");

        let deserialized: LocalLogMetadata =
            ciborium::from_reader(&buffer[..]).expect("Failed to deserialize");

        assert_eq!(deserialized.term, metadata.term);
        assert_eq!(deserialized.group_id, metadata.group_id);
    }

    #[tokio::test]
    async fn test_local_entry_types() {
        let entry_types = vec![
            LocalEntryType::StreamOperation {
                stream_id: "stream1".to_string(),
                operation: StreamOperationType::Publish,
            },
            LocalEntryType::StreamManagement,
            LocalEntryType::MembershipChange,
            LocalEntryType::Migration,
            LocalEntryType::Empty,
        ];

        for entry_type in entry_types {
            let metadata = LocalLogMetadata {
                term: 1,
                leader_node_id: NodeId::from_seed(1),
                entry_type: entry_type.clone(),
                group_id: ConsensusGroupId(1),
            };

            // Test that all entry types can be serialized
            let mut buffer = Vec::new();
            ciborium::into_writer(&metadata, &mut buffer).expect("Failed to serialize");

            let deserialized: LocalLogMetadata =
                ciborium::from_reader(&buffer[..]).expect("Failed to deserialize");

            // Verify entry type matches
            match (entry_type, deserialized.entry_type) {
                (
                    LocalEntryType::StreamOperation { stream_id: s1, .. },
                    LocalEntryType::StreamOperation { stream_id: s2, .. },
                ) => {
                    assert_eq!(s1, s2);
                }
                (LocalEntryType::StreamManagement, LocalEntryType::StreamManagement) => {}
                (LocalEntryType::MembershipChange, LocalEntryType::MembershipChange) => {}
                (LocalEntryType::Migration, LocalEntryType::Migration) => {}
                (LocalEntryType::Empty, LocalEntryType::Empty) => {}
                _ => panic!("Entry type mismatch"),
            }
        }
    }
}
