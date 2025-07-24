//! Time-related types

use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

/// Timestamp type for consensus operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ConsensusTimestamp {
    /// Seconds since epoch
    secs: u64,
    /// Nanoseconds
    nanos: u32,
}

impl ConsensusTimestamp {
    /// Create a new timestamp from current time
    pub fn now() -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("System time before epoch");
        Self {
            secs: now.as_secs(),
            nanos: now.subsec_nanos(),
        }
    }

    /// Create from duration since epoch
    pub fn from_duration(duration: Duration) -> Self {
        Self {
            secs: duration.as_secs(),
            nanos: duration.subsec_nanos(),
        }
    }

    /// Get duration since epoch
    pub fn duration_since_epoch(&self) -> Duration {
        Duration::new(self.secs, self.nanos)
    }
}
