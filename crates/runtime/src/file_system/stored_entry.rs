use super::entry::Entry;
use super::metadata::FsMetadata;

use bytes::Bytes;
use proven_store::Store;
use serde::{Deserialize, Serialize};

/// Repsents a file system entry. This is the serializable version to be stored inside a KV `Store`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum StoredEntry {
    /// Represents a directory entry.
    Directory {
        /// The metadata of the directory.
        metadata: FsMetadata,
    },

    /// Represents a file entry.
    File {
        /// The metadata of the file.
        metadata: FsMetadata,

        /// The content of the file.
        content: Bytes,
    },

    /// Represents a symbolic link.
    Symlink {
        /// The metadata of the symlink.
        metadata: FsMetadata,

        /// The target path this symlink points to.
        target: String,
    },
}

impl<S> From<Entry<S>> for StoredEntry
where
    S: Store<Self, serde_json::Error, serde_json::Error>,
{
    fn from(entry: Entry<S>) -> Self {
        match entry {
            Entry::Directory { metadata } => Self::Directory { metadata },
            Entry::File(file) => Self::File {
                metadata: file.metadata(),
                content: file.content().into(),
            },
            Entry::Symlink { metadata, target } => Self::Symlink { metadata, target },
        }
    }
}

#[allow(dead_code)]
impl StoredEntry {
    pub(crate) const fn content(&self) -> Option<&Bytes> {
        match self {
            Self::File { content, .. } => Some(content),
            _ => None,
        }
    }

    pub(crate) fn content_mut(&mut self) -> Option<&mut Bytes> {
        match self {
            Self::File { content, .. } => Some(content),
            _ => None,
        }
    }

    pub(crate) const fn metadata(&self) -> &FsMetadata {
        match self {
            Self::Directory { metadata, .. }
            | Self::File { metadata, .. }
            | Self::Symlink { metadata, .. } => metadata,
        }
    }

    pub(crate) const fn metadata_mut(&mut self) -> &mut FsMetadata {
        match self {
            Self::Directory { metadata, .. }
            | Self::File { metadata, .. }
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

impl TryFrom<Bytes> for StoredEntry {
    type Error = serde_json::Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        serde_json::from_slice(&bytes)
    }
}

impl TryFrom<StoredEntry> for Bytes {
    type Error = serde_json::Error;

    fn try_from(storage_entry: StoredEntry) -> Result<Self, Self::Error> {
        serde_json::to_vec(&storage_entry).map(Self::from)
    }
}
