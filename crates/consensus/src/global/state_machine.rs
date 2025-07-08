//! State machine for consensus operations
//!
//! DEPRECATED: This module is being refactored. Use global_state::GlobalState instead.
//! StreamStore is now an alias for GlobalState for backward compatibility.

// Re-export types from global_state for backward compatibility
pub use super::global_state::{
    ConsensusGroupInfo, GlobalState as StreamStore, MessageData, MessageSource, PubSubDetails,
    StreamData, validate_stream_name,
};
