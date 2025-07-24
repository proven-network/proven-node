//! State management and access control

pub mod access;
pub mod global_state;
pub mod group_state;

// Re-export commonly used items
pub use access::{
    GlobalStateRead, GlobalStateReader, GlobalStateWrite, GlobalStateWriter, GroupStateRead,
    GroupStateReader, GroupStateWrite, GroupStateWriter, create_group_state_access,
    create_state_access,
};
pub use global_state::GlobalState;
pub use group_state::GroupState;
