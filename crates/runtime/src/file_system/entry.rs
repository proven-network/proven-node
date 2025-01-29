use super::file::File;
use super::metadata::FsMetadata;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Represents a file system entry. Stored inside a KV `Store`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Entry {
    /// Represents a directory entry.
    Directory {
        /// The metadata of the directory.
        metadata: FsMetadata,
    },
    /// Represents a file entry.
    File(File),
    /// Represents a symbolic link
    Symlink {
        /// The metadata of the symlink
        metadata: FsMetadata,
        /// The target path this symlink points to
        target: String,
    },
}

#[allow(dead_code)]
impl Entry {
    pub(crate) const fn metadata(&self) -> &FsMetadata {
        match self {
            Self::Directory { metadata, .. }
            | Self::File(File { metadata, .. })
            | Self::Symlink { metadata, .. } => metadata,
        }
    }

    pub(crate) const fn metadata_mut(&mut self) -> &mut FsMetadata {
        match self {
            Self::Directory { metadata, .. }
            | Self::File(File { metadata, .. })
            | Self::Symlink { metadata, .. } => metadata,
        }
    }

    pub(crate) const fn is_symlink(&self) -> bool {
        matches!(self, Self::Symlink { .. })
    }

    pub(crate) fn symlink_target(&self) -> Option<&str> {
        match self {
            Self::Symlink { target, .. } => Some(target),
            _ => None,
        }
    }
}

impl TryFrom<Bytes> for Entry {
    type Error = serde_json::Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        serde_json::from_slice(&bytes)
    }
}

impl TryFrom<Entry> for Bytes {
    type Error = serde_json::Error;

    fn try_from(entry: Entry) -> Result<Self, Self::Error> {
        serde_json::to_vec(&entry).map(Self::from)
    }
}
