//! Log index conversion utilities for OpenRaft
//!
//! This module provides conversion between our 1-based LogIndex and OpenRaft's 0-based u64

use proven_storage::LogIndex;

/// Extension trait for LogIndex to provide OpenRaft conversion
pub trait LogIndexExt {
    /// Convert to OpenRaft's 0-based u64
    fn to_openraft_u64(&self) -> u64;
}

impl LogIndexExt for LogIndex {
    fn to_openraft_u64(&self) -> u64 {
        self.get() - 1
    }
}

/// Extension trait for Option<LogIndex> to provide OpenRaft conversion
pub trait OptionLogIndexExt {
    /// Convert to OpenRaft's Option<u64> (0-based)
    fn to_openraft_u64(&self) -> Option<u64>;
}

impl OptionLogIndexExt for Option<LogIndex> {
    fn to_openraft_u64(&self) -> Option<u64> {
        self.map(|idx| idx.to_openraft_u64())
    }
}

/// Convert from OpenRaft's u64 to LogIndex (0-based to 1-based)
pub fn from_openraft_u64(index: u64) -> Option<LogIndex> {
    index.checked_add(1).and_then(LogIndex::new)
}

/// Convert from OpenRaft's Option<u64> to Option<LogIndex>
pub fn from_openraft_option(index: Option<u64>) -> Option<LogIndex> {
    index.and_then(from_openraft_u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conversions() {
        // Test 1-based to 0-based
        let log_idx = LogIndex::new(1).unwrap();
        assert_eq!(log_idx.to_openraft_u64(), 0);

        // Test 0-based to 1-based
        let log_idx = from_openraft_u64(0).unwrap();
        assert_eq!(log_idx.get(), 1);

        // Test round trip
        let original = LogIndex::new(42).unwrap();
        let converted = original.to_openraft_u64();
        let back = from_openraft_u64(converted).unwrap();
        assert_eq!(original, back);
    }

    #[test]
    fn test_option_conversion() {
        let some_idx = Some(LogIndex::new(5).unwrap());
        assert_eq!(some_idx.to_openraft_u64(), Some(4));

        let none_idx: Option<LogIndex> = None;
        assert_eq!(none_idx.to_openraft_u64(), None);
    }

    #[test]
    fn test_from_openraft_option() {
        let some_u64 = Some(4);
        let converted = from_openraft_option(some_u64).unwrap();
        assert_eq!(converted.get(), 5);

        let none_u64: Option<u64> = None;
        assert_eq!(from_openraft_option(none_u64), None);
    }
}
