//! Data models used throughout the consensus system

pub mod global;
pub mod metadata;
pub mod stream;

// Re-export commonly used types
pub use global::{GroupInfo, GroupStateInfo, NodeInfo};
pub use metadata::GroupMetadata;
pub use stream::{
    PersistenceType, RetentionPolicy, StreamConfig, StreamInfo, StreamState, StreamStats,
};
