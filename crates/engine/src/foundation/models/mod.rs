//! Data models used throughout the consensus system

pub mod global;
pub mod metadata;
pub mod stream;

// Re-export commonly used types
pub use global::{GroupInfo, GroupStateInfo, GroupStreamInfo, NodeInfo};
pub use metadata::GroupMetadata;
pub use stream::{
    PersistenceType, RetentionPolicy, StoredMessage, StreamConfig, StreamInfo, StreamState,
    StreamStats, deserialize_stored_message, serialize_stored_message,
};
