//! Data models used throughout the consensus system

pub mod global;
pub mod metadata;
pub mod stream;

// Re-export commonly used types
pub use global::{GroupInfo, NodeInfo};
pub use metadata::GroupMetadata;
pub use stream::{StreamInfo, StreamState, StreamStats};
