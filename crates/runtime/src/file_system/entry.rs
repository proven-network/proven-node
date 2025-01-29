use super::file::{File, StorageFile};
use super::metadata::FsMetadata;

use bytes::Bytes;
use proven_store::Store;
use serde::{Deserialize, Serialize};

/// Repsents a file system entry. This is the serializable version to be stored inside a KV `Store`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum StorageEntry {
    /// Represents a directory entry.
    Directory {
        /// The metadata of the directory.
        metadata: FsMetadata,
    },

    /// Represents a file entry.
    File(StorageFile),

    /// Represents a symbolic link.
    Symlink {
        /// The metadata of the symlink.
        metadata: FsMetadata,

        /// The target path this symlink points to.
        target: String,
    },
}

/// Represents a file system entry.
#[derive(Clone, Debug)]
pub enum Entry<S>
where
    S: Store<StorageEntry, serde_json::Error, serde_json::Error>,
{
    /// Represents a directory entry.
    Directory {
        /// The metadata of the directory.
        metadata: FsMetadata,
    },

    /// Represents a file entry.
    File(File<S>),

    /// Represents a symbolic link
    Symlink {
        /// The metadata of the symlink
        metadata: FsMetadata,

        /// The target path this symlink points to
        target: String,
    },
}

impl<S> From<StorageEntry> for Entry<S>
where
    S: Store<StorageEntry, serde_json::Error, serde_json::Error>,
{
    fn from(storage: StorageEntry) -> Self {
        match storage {
            StorageEntry::Directory { metadata } => Self::Directory { metadata },
            StorageEntry::File(file) => Self::File(file.into()),
            StorageEntry::Symlink { metadata, target } => Self::Symlink { metadata, target },
        }
    }
}

impl<S> From<Entry<S>> for StorageEntry
where
    S: Store<Self, serde_json::Error, serde_json::Error>,
{
    fn from(entry: Entry<S>) -> Self {
        match entry {
            Entry::Directory { metadata } => Self::Directory { metadata },
            Entry::File(file) => Self::File(file.into()),
            Entry::Symlink { metadata, target } => Self::Symlink { metadata, target },
        }
    }
}

#[allow(dead_code)]
impl<S> Entry<S>
where
    S: Store<StorageEntry, serde_json::Error, serde_json::Error>,
{
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

impl TryFrom<Bytes> for StorageEntry {
    type Error = serde_json::Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        serde_json::from_slice(&bytes)
    }
}

impl TryFrom<StorageEntry> for Bytes {
    type Error = serde_json::Error;

    fn try_from(storage_entry: StorageEntry) -> Result<Self, Self::Error> {
        serde_json::to_vec(&storage_entry).map(Self::from)
    }
}
