//! Network service for managing consensus network operations
//!
//! This service encapsulates all network operations and provides
//! a clean interface between the consensus engine and the external
//! NetworkManager.

mod service;
mod types;

pub use service::{NetworkConfig, NetworkService};
pub use types::{ConnectionState, NetworkError, NetworkEvent, NetworkResult, NetworkStats};
