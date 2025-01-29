use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetadata {
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mtime: Option<u64>,     // milliseconds since epoch
    pub atime: Option<u64>,     // milliseconds since epoch
    pub birthtime: Option<u64>, // milliseconds since epoch
    pub ctime: Option<u64>,     // milliseconds since epoch
}
