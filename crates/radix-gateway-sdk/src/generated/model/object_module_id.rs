use serde::{Deserialize, Serialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ObjectModuleId {
    Main,
    Metadata,
    Royalty,
    RoleAssignment,
}
