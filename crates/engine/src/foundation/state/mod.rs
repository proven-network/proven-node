//! State management and access control

pub mod global_access;
pub mod global_state;
pub mod group_access;
pub mod group_state;

// Re-export commonly used items
pub use global_access::{
    GlobalStateRead, GlobalStateReader, GlobalStateWrite, GlobalStateWriter,
    create_global_state_access,
};
pub use global_state::GlobalState;

pub use group_access::{
    GroupStateRead, GroupStateReader, GroupStateWrite, GroupStateWriter, create_group_state_access,
};
pub use group_state::GroupState;
