use serde::{Serialize, Deserialize};
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ObjectModuleId {
    Main,
    Metadata,
    Royalty,
    RoleAssignment,
}
