use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetadata {
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub modified: (i64, u32), // seconds, nanos
    pub accessed: (i64, u32),
    pub created: (i64, u32),
}
