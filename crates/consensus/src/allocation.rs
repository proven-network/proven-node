use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Identifier for a local consensus group
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConsensusGroupId(pub u32);

impl ConsensusGroupId {
    /// Create a new consensus group ID
    pub fn new(id: u32) -> Self {
        Self(id)
    }
}

/// Represents the allocation of a stream to a consensus group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamAllocation {
    /// The name of the stream
    pub stream_name: String,
    /// The consensus group the stream is allocated to
    pub group_id: ConsensusGroupId,
    /// The timestamp when the stream was allocated
    pub allocated_at: u64,
    /// The timestamp when the stream was last migrated
    pub last_migrated: Option<u64>,
}

/// Allocation strategy for mapping streams to consensus groups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AllocationStrategy {
    /// Consistent hashing based on stream name
    ConsistentHash {
        /// The number of virtual nodes to use for consistent hashing
        virtual_nodes: u32,
    },
    /// Round-robin allocation
    RoundRobin,
    /// Manual assignment
    Manual,
}

/// Manages stream allocations across consensus groups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationManager {
    /// Map of stream names to their allocated groups
    allocations: HashMap<String, StreamAllocation>,
    /// Available consensus groups
    groups: HashMap<ConsensusGroupId, GroupMetadata>,
    /// Allocation strategy
    strategy: AllocationStrategy,
    /// Next group for round-robin
    #[serde(default)]
    next_group_index: u32,
}

/// Metadata about a consensus group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMetadata {
    /// The ID of the consensus group
    pub id: ConsensusGroupId,
    /// The number of streams allocated to the group
    pub stream_count: u32,
    /// The average message rate of the group
    pub message_rate: f64,
    /// The storage size of the group
    pub storage_size: u64,
    /// The members of the group
    pub members: Vec<crate::NodeId>,
}

impl AllocationManager {
    /// Create a new allocation manager
    pub fn new(strategy: AllocationStrategy) -> Self {
        Self {
            allocations: HashMap::new(),
            groups: HashMap::new(),
            strategy,
            next_group_index: 0,
        }
    }

    /// Add a new consensus group
    pub fn add_group(&mut self, id: ConsensusGroupId, members: Vec<crate::NodeId>) {
        self.groups.insert(
            id,
            GroupMetadata {
                id,
                stream_count: 0,
                message_rate: 0.0,
                storage_size: 0,
                members,
            },
        );
    }

    /// Remove a consensus group (must have no allocated streams)
    pub fn remove_group(&mut self, id: ConsensusGroupId) -> Result<(), String> {
        if let Some(metadata) = self.groups.get(&id) {
            if metadata.stream_count > 0 {
                return Err("Cannot remove group with allocated streams".to_string());
            }
        }
        self.groups.remove(&id);
        Ok(())
    }

    /// Allocate a stream to a consensus group
    pub fn allocate_stream(
        &mut self,
        stream_name: String,
        timestamp: u64,
    ) -> Result<ConsensusGroupId, String> {
        if self.groups.is_empty() {
            return Err("No consensus groups available".to_string());
        }

        if self.allocations.contains_key(&stream_name) {
            return Err("Stream already allocated".to_string());
        }

        let group_id = match &self.strategy {
            AllocationStrategy::ConsistentHash { virtual_nodes } => {
                self.consistent_hash_allocation(&stream_name, *virtual_nodes)
            }
            AllocationStrategy::RoundRobin => self.round_robin_allocation(),
            AllocationStrategy::Manual => {
                return Err("Manual allocation requires explicit group assignment".to_string());
            }
        };

        let allocation = StreamAllocation {
            stream_name: stream_name.clone(),
            group_id,
            allocated_at: timestamp,
            last_migrated: None,
        };

        self.allocations.insert(stream_name, allocation);

        if let Some(metadata) = self.groups.get_mut(&group_id) {
            metadata.stream_count += 1;
        }

        Ok(group_id)
    }

    /// Get the consensus group for a stream
    pub fn get_stream_group(&self, stream_name: &str) -> Option<ConsensusGroupId> {
        self.allocations.get(stream_name).map(|a| a.group_id)
    }

    /// Migrate a stream to a different consensus group
    pub fn migrate_stream(
        &mut self,
        stream_name: &str,
        new_group_id: ConsensusGroupId,
        timestamp: u64,
    ) -> Result<(), String> {
        let allocation = self
            .allocations
            .get_mut(stream_name)
            .ok_or("Stream not found")?;

        let old_group_id = allocation.group_id;

        if !self.groups.contains_key(&new_group_id) {
            return Err("Target group does not exist".to_string());
        }

        // Update allocation
        allocation.group_id = new_group_id;
        allocation.last_migrated = Some(timestamp);

        // Update group metadata
        if let Some(old_metadata) = self.groups.get_mut(&old_group_id) {
            old_metadata.stream_count = old_metadata.stream_count.saturating_sub(1);
        }

        if let Some(new_metadata) = self.groups.get_mut(&new_group_id) {
            new_metadata.stream_count += 1;
        }

        Ok(())
    }

    /// Get load statistics for all groups
    pub fn get_group_loads(&self) -> Vec<(ConsensusGroupId, GroupMetadata)> {
        self.groups
            .iter()
            .map(|(id, metadata)| (*id, metadata.clone()))
            .collect()
    }

    /// Find the least loaded group based on stream count
    pub fn find_least_loaded_group(&self) -> Option<ConsensusGroupId> {
        self.groups
            .values()
            .min_by_key(|m| m.stream_count)
            .map(|m| m.id)
    }

    /// Update the message rate for a consensus group
    pub fn update_group_message_rate(
        &mut self,
        group_id: ConsensusGroupId,
        message_rate: f64,
    ) -> Result<(), String> {
        if let Some(metadata) = self.groups.get_mut(&group_id) {
            metadata.message_rate = message_rate;
            Ok(())
        } else {
            Err(format!("Group {:?} not found", group_id))
        }
    }

    /// Update storage size for a consensus group
    pub fn update_group_storage_size(
        &mut self,
        group_id: ConsensusGroupId,
        storage_size: u64,
    ) -> Result<(), String> {
        if let Some(metadata) = self.groups.get_mut(&group_id) {
            metadata.storage_size = storage_size;
            Ok(())
        } else {
            Err(format!("Group {:?} not found", group_id))
        }
    }

    /// Reallocate a stream to a different consensus group
    pub fn reallocate_stream(
        &mut self,
        stream_name: &str,
        new_group_id: ConsensusGroupId,
    ) -> Result<(), String> {
        // Check if the stream exists
        let old_group_id = match self.allocations.get(stream_name) {
            Some(allocation) => allocation.group_id,
            None => return Err(format!("Stream '{}' not found", stream_name)),
        };

        // Check if the new group exists
        if !self.groups.contains_key(&new_group_id) {
            return Err(format!("Group {:?} not found", new_group_id));
        }

        // Update the allocation
        if let Some(allocation) = self.allocations.get_mut(stream_name) {
            allocation.group_id = new_group_id;
            allocation.last_migrated = Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
        }

        // Update stream counts
        if let Some(old_metadata) = self.groups.get_mut(&old_group_id) {
            old_metadata.stream_count = old_metadata.stream_count.saturating_sub(1);
        }

        if let Some(new_metadata) = self.groups.get_mut(&new_group_id) {
            new_metadata.stream_count += 1;
        }

        Ok(())
    }

    // Allocation strategies

    fn consistent_hash_allocation(
        &self,
        stream_name: &str,
        virtual_nodes: u32,
    ) -> ConsensusGroupId {
        let mut best_group = None;
        let mut best_hash = u64::MAX;

        let stream_hash = self.hash_string(stream_name);

        for group_id in self.groups.keys() {
            for i in 0..virtual_nodes {
                let node_key = format!("{}-{}", group_id.0, i);
                let node_hash = self.hash_string(&node_key);

                if node_hash >= stream_hash && node_hash < best_hash {
                    best_hash = node_hash;
                    best_group = Some(*group_id);
                }
            }
        }

        // If no node found with hash >= stream_hash, wrap around to the smallest
        best_group.unwrap_or_else(|| {
            self.groups
                .keys()
                .min_by_key(|group_id| {
                    let node_key = format!("{}-0", group_id.0);
                    self.hash_string(&node_key)
                })
                .copied()
                .unwrap()
        })
    }

    fn round_robin_allocation(&mut self) -> ConsensusGroupId {
        let group_ids: Vec<_> = self.groups.keys().copied().collect();
        let index = (self.next_group_index as usize) % group_ids.len();
        self.next_group_index = self.next_group_index.wrapping_add(1);
        group_ids[index]
    }

    fn hash_string(&self, s: &str) -> u64 {
        let mut hasher = Sha256::new();
        hasher.update(s.as_bytes());
        let result = hasher.finalize();

        // Take first 8 bytes as u64
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&result[..8]);
        u64::from_be_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::NodeId;

    use super::*;

    #[test]
    fn test_allocation_manager() {
        let mut manager = AllocationManager::new(AllocationStrategy::RoundRobin);

        // Add groups
        manager.add_group(
            ConsensusGroupId::new(1),
            vec![
                NodeId::from_seed(1),
                NodeId::from_seed(2),
                NodeId::from_seed(3),
            ],
        );
        manager.add_group(
            ConsensusGroupId::new(2),
            vec![
                NodeId::from_seed(2),
                NodeId::from_seed(3),
                NodeId::from_seed(4),
            ],
        );

        // Allocate streams
        let group1 = manager.allocate_stream("stream1".to_string(), 100).unwrap();
        let group2 = manager.allocate_stream("stream2".to_string(), 101).unwrap();

        // Should round-robin
        assert_ne!(group1, group2);

        // Get allocation
        assert_eq!(manager.get_stream_group("stream1"), Some(group1));

        // Test migration
        manager.migrate_stream("stream1", group2, 200).unwrap();
        assert_eq!(manager.get_stream_group("stream1"), Some(group2));
    }

    #[test]
    fn test_consistent_hash() {
        let mut manager =
            AllocationManager::new(AllocationStrategy::ConsistentHash { virtual_nodes: 100 });

        manager.add_group(
            ConsensusGroupId::new(1),
            vec![
                NodeId::from_seed(1),
                NodeId::from_seed(2),
                NodeId::from_seed(3),
            ],
        );
        manager.add_group(
            ConsensusGroupId::new(2),
            vec![
                NodeId::from_seed(2),
                NodeId::from_seed(3),
                NodeId::from_seed(4),
            ],
        );
        manager.add_group(
            ConsensusGroupId::new(3),
            vec![
                NodeId::from_seed(3),
                NodeId::from_seed(4),
                NodeId::from_seed(5),
            ],
        );

        // Same stream should always map to same group
        let group1 = manager
            .allocate_stream("test-stream".to_string(), 100)
            .unwrap();
        manager.allocations.clear(); // Clear to test again
        manager.groups.get_mut(&group1).unwrap().stream_count = 0;

        let group2 = manager
            .allocate_stream("test-stream".to_string(), 200)
            .unwrap();
        assert_eq!(group1, group2);
    }

    #[tokio::test]
    async fn test_stream_allocation() {
        let mut manager = AllocationManager::new(AllocationStrategy::RoundRobin);

        // Add groups
        manager.add_group(
            ConsensusGroupId::new(0),
            vec![
                NodeId::from_seed(1),
                NodeId::from_seed(2),
                NodeId::from_seed(3),
            ],
        );
        manager.add_group(
            ConsensusGroupId::new(1),
            vec![
                NodeId::from_seed(2),
                NodeId::from_seed(3),
                NodeId::from_seed(4),
            ],
        );
        manager.add_group(
            ConsensusGroupId::new(2),
            vec![
                NodeId::from_seed(3),
                NodeId::from_seed(4),
                NodeId::from_seed(5),
            ],
        );

        // Allocate streams
        let group1 = manager.allocate_stream("stream1".to_string(), 100).unwrap();
        let group2 = manager.allocate_stream("stream2".to_string(), 101).unwrap();
        let group3 = manager.allocate_stream("stream3".to_string(), 102).unwrap();

        // Round-robin should distribute evenly
        assert_ne!(group1, group2);
        assert_ne!(group2, group3);

        // Verify allocations
        assert_eq!(manager.get_stream_group("stream1"), Some(group1));
        assert_eq!(manager.get_stream_group("stream2"), Some(group2));
        assert_eq!(manager.get_stream_group("stream3"), Some(group3));
    }

    #[tokio::test]
    async fn test_consistent_hash_allocation() {
        let mut manager =
            AllocationManager::new(AllocationStrategy::ConsistentHash { virtual_nodes: 100 });

        manager.add_group(
            ConsensusGroupId::new(0),
            vec![
                NodeId::from_seed(1),
                NodeId::from_seed(2),
                NodeId::from_seed(3),
            ],
        );
        manager.add_group(
            ConsensusGroupId::new(1),
            vec![
                NodeId::from_seed(2),
                NodeId::from_seed(3),
                NodeId::from_seed(4),
            ],
        );

        // Same stream should always map to same group
        let group1 = manager
            .allocate_stream("test-stream".to_string(), 100)
            .unwrap();

        // Get the stream's group to verify consistent hashing
        let stream_group = manager.get_stream_group("test-stream").unwrap();
        assert_eq!(
            group1, stream_group,
            "Stream should be allocated to the expected group"
        );
    }

    #[tokio::test]
    async fn test_stream_migration() {
        let mut manager = AllocationManager::new(AllocationStrategy::RoundRobin);

        let group0 = ConsensusGroupId::new(0);
        let group1 = ConsensusGroupId::new(1);

        manager.add_group(
            group0,
            vec![
                NodeId::from_seed(1),
                NodeId::from_seed(2),
                NodeId::from_seed(3),
            ],
        );
        manager.add_group(
            group1,
            vec![
                NodeId::from_seed(2),
                NodeId::from_seed(3),
                NodeId::from_seed(4),
            ],
        );

        // Allocate stream to group0
        let allocated_group = manager.allocate_stream("stream1".to_string(), 100).unwrap();

        // Check initial load distribution
        let initial_loads = manager.get_group_loads();
        let initial_count = initial_loads
            .iter()
            .find(|(id, _)| *id == allocated_group)
            .map(|(_, m)| m.stream_count)
            .unwrap();
        assert_eq!(initial_count, 1);

        // Migrate to group1
        manager.migrate_stream("stream1", group1, 200).unwrap();

        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Verify migration
        assert_eq!(manager.get_stream_group("stream1"), Some(group1));

        // Check final load distribution
        let final_loads = manager.get_group_loads();
        let old_count = final_loads
            .iter()
            .find(|(id, _)| *id == allocated_group)
            .map(|(_, m)| m.stream_count)
            .unwrap();
        let new_count = final_loads
            .iter()
            .find(|(id, _)| *id == group1)
            .map(|(_, m)| m.stream_count)
            .unwrap();
        assert_eq!(old_count, 0);
        assert_eq!(new_count, 1);
    }

    #[test]
    fn test_load_balancing() {
        let mut manager = AllocationManager::new(AllocationStrategy::RoundRobin);

        // Add groups
        for i in 0..3 {
            manager.add_group(
                ConsensusGroupId::new(i),
                vec![
                    NodeId::from_seed(1),
                    NodeId::from_seed(2),
                    NodeId::from_seed(3),
                ],
            );
        }

        // Allocate many streams
        for i in 0..30 {
            manager
                .allocate_stream(format!("stream-{}", i), 1000 + i)
                .unwrap();
        }

        // Check distribution
        let loads = manager.get_group_loads();
        for (_, metadata) in &loads {
            // Round-robin should distribute evenly (10 each)
            assert_eq!(metadata.stream_count, 10);
        }

        // Find least loaded (they're all equal, but this tests the method)
        let least = manager.find_least_loaded_group().unwrap();
        assert!(loads.iter().any(|(id, _)| *id == least));
    }

    #[tokio::test]
    async fn test_migration_lifecycle() {
        // Create allocation manager
        let mut allocation_manager = AllocationManager::new(AllocationStrategy::RoundRobin);

        // Add two consensus groups
        let group0 = ConsensusGroupId::new(0);
        let group1 = ConsensusGroupId::new(1);

        allocation_manager.add_group(
            group0,
            vec![
                NodeId::from_seed(1),
                NodeId::from_seed(2),
                NodeId::from_seed(3),
            ],
        );
        allocation_manager.add_group(
            group1,
            vec![
                NodeId::from_seed(4),
                NodeId::from_seed(5),
                NodeId::from_seed(6),
            ],
        );

        // Allocate a stream (round-robin will choose the first available group)
        let stream_name = "test-stream";
        let allocated_group = allocation_manager
            .allocate_stream(stream_name.to_string(), 100)
            .unwrap();
        // Round-robin allocation may allocate to either group0 or group1 first
        assert!(allocated_group == group0 || allocated_group == group1);

        // Verify initial allocation
        assert_eq!(
            allocation_manager.get_stream_group(stream_name),
            Some(allocated_group)
        );

        // Start migration to the other group
        let target_group = if allocated_group == group0 {
            group1
        } else {
            group0
        };
        allocation_manager
            .migrate_stream(stream_name, target_group, 200)
            .unwrap();

        // Verify stream is now in target group
        assert_eq!(
            allocation_manager.get_stream_group(stream_name),
            Some(target_group)
        );

        // Verify load distribution after migration
        let loads = allocation_manager.get_group_loads();
        let source_load = loads
            .iter()
            .find(|(id, _)| *id == allocated_group)
            .map(|(_, m)| m.stream_count)
            .unwrap();
        let target_load = loads
            .iter()
            .find(|(id, _)| *id == target_group)
            .map(|(_, m)| m.stream_count)
            .unwrap();

        assert_eq!(source_load, 0);
        assert_eq!(target_load, 1);
    }

    #[tokio::test]
    async fn test_concurrent_migrations() {
        let mut manager = AllocationManager::new(AllocationStrategy::RoundRobin);

        // Add three groups
        for i in 0..3 {
            manager.add_group(
                ConsensusGroupId::new(i),
                vec![
                    NodeId::from_seed((i * 3 + 1) as u8),
                    NodeId::from_seed((i * 3 + 2) as u8),
                    NodeId::from_seed((i * 3 + 3) as u8),
                ],
            );
        }

        // Allocate multiple streams
        let streams: Vec<_> = (0..6).map(|i| format!("stream-{}", i)).collect();

        for (i, stream) in streams.iter().enumerate() {
            manager
                .allocate_stream(stream.to_string(), 1000 + i as u64)
                .unwrap();
        }

        // Migrate different streams to different groups
        manager
            .migrate_stream(&streams[0], ConsensusGroupId::new(1), 2000)
            .unwrap();
        manager
            .migrate_stream(&streams[1], ConsensusGroupId::new(2), 2001)
            .unwrap();
        manager
            .migrate_stream(&streams[2], ConsensusGroupId::new(0), 2002)
            .unwrap();

        // Verify each migration completed correctly
        assert_eq!(
            manager.get_stream_group(&streams[0]),
            Some(ConsensusGroupId::new(1))
        );
        assert_eq!(
            manager.get_stream_group(&streams[1]),
            Some(ConsensusGroupId::new(2))
        );
        assert_eq!(
            manager.get_stream_group(&streams[2]),
            Some(ConsensusGroupId::new(0))
        );

        // Verify load is still balanced
        let loads = manager.get_group_loads();
        for (_, metadata) in loads {
            assert_eq!(metadata.stream_count, 2); // Each group should have 2 streams
        }
    }
}
