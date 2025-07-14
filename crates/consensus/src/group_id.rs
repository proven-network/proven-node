//! Group ID type

use std::fmt;

use serde::{Deserialize, Serialize};

/// Identifier for a local consensus group
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConsensusGroupId(pub u32);

impl ConsensusGroupId {
    /// Create a new consensus group ID
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    /// Get the initial consensus group ID (group 0)
    pub fn initial() -> Self {
        Self(0)
    }

    /// Get the numeric ID value
    pub fn value(&self) -> u32 {
        self.0
    }

    /// Check if this is the initial group
    pub fn is_initial(&self) -> bool {
        self.0 == 0
    }

    /// Get the next group ID in sequence
    pub fn next(&self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Get the previous group ID in sequence
    pub fn previous(&self) -> Option<Self> {
        if self.0 > 0 {
            Some(Self(self.0 - 1))
        } else {
            None
        }
    }

    /// Create a group ID from a hash for consistent allocation
    pub fn from_hash(hash: &[u8], total_groups: u32) -> Self {
        if total_groups == 0 {
            return Self::initial();
        }

        // Use first 4 bytes of hash for group selection
        let value = if hash.len() >= 4 {
            u32::from_be_bytes([hash[0], hash[1], hash[2], hash[3]])
        } else {
            // Fallback for short hashes
            let mut bytes = [0u8; 4];
            bytes[..hash.len()].copy_from_slice(hash);
            u32::from_be_bytes(bytes)
        };

        Self(value % total_groups)
    }

    /// Check if this group ID is valid (within expected range)
    pub fn is_valid(&self, max_groups: u32) -> bool {
        self.0 < max_groups
    }
}

impl fmt::Display for ConsensusGroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "group-{}", self.0)
    }
}

impl From<u32> for ConsensusGroupId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl From<ConsensusGroupId> for u32 {
    fn from(group_id: ConsensusGroupId) -> Self {
        group_id.0
    }
}

impl std::ops::Add<u32> for ConsensusGroupId {
    type Output = Self;

    fn add(self, rhs: u32) -> Self::Output {
        Self(self.0.saturating_add(rhs))
    }
}

impl std::ops::Sub<u32> for ConsensusGroupId {
    type Output = Option<Self>;

    fn sub(self, rhs: u32) -> Self::Output {
        self.0.checked_sub(rhs).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_id_creation() {
        let group = ConsensusGroupId::new(42);
        assert_eq!(group.value(), 42);
        assert_eq!(group.to_string(), "group-42");
    }

    #[test]
    fn test_initial_group() {
        let group = ConsensusGroupId::initial();
        assert_eq!(group.value(), 0);
        assert!(group.is_initial());
        assert_eq!(group.to_string(), "group-0");
    }

    #[test]
    fn test_next_previous() {
        let group = ConsensusGroupId::new(5);
        assert_eq!(group.next().value(), 6);
        assert_eq!(group.previous().unwrap().value(), 4);

        let initial = ConsensusGroupId::initial();
        assert_eq!(initial.next().value(), 1);
        assert!(initial.previous().is_none());
    }

    #[test]
    fn test_from_hash() {
        let hash = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let group = ConsensusGroupId::from_hash(&hash, 10);
        assert!(group.value() < 10);

        // Test with zero groups
        let group_zero = ConsensusGroupId::from_hash(&hash, 0);
        assert_eq!(group_zero.value(), 0);

        // Test with short hash
        let short_hash = [1u8, 2];
        let group_short = ConsensusGroupId::from_hash(&short_hash, 10);
        assert!(group_short.value() < 10);
    }

    #[test]
    fn test_validity() {
        let group = ConsensusGroupId::new(5);
        assert!(group.is_valid(10));
        assert!(!group.is_valid(5));
        assert!(!group.is_valid(3));
    }

    #[test]
    fn test_arithmetic() {
        let group = ConsensusGroupId::new(5);
        assert_eq!((group + 3).value(), 8);
        assert_eq!((group - 2).unwrap().value(), 3);
        assert!((group - 10).is_none());

        // Test saturation
        let max_group = ConsensusGroupId::new(u32::MAX);
        assert_eq!((max_group + 1).value(), u32::MAX);
    }

    #[test]
    fn test_conversions() {
        let id: u32 = 42;
        let group: ConsensusGroupId = id.into();
        assert_eq!(group.value(), 42);

        let back: u32 = group.into();
        assert_eq!(back, 42);
    }
}
