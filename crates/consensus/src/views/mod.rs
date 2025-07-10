//! Read-only views for orchestrator decision-making
//!
//! This module contains specialized view components that provide read-only
//! access to system state for the orchestrator. These views do not modify
//! state directly - all modifications must go through consensus operations.
//!
//! - StreamView: Read-only view of stream state and health
//! - GroupView: Read-only view of consensus group state and health
//! - NodeView: Read-only view of node state and capacity

pub mod group_view;
pub mod local_stream_view;
pub mod node_view;
pub mod stream_management_view;

pub use group_view::GroupView;
pub use local_stream_view::LocalStreamView;
pub use node_view::NodeView;
pub use stream_management_view::StreamManagementView;
