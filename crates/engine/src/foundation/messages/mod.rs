//! Message types and serialization

pub mod format;
pub mod types;

// Re-export commonly used items
pub use format::{deserialize_entry, serialize_entry};
pub use types::{Message, headers};
