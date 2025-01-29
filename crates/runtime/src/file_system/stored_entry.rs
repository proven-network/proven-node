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
    S: Store<Self, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
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
    type Error = ciborium::de::Error<std::io::Error>;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        ciborium::de::from_reader(bytes.as_ref())
    }
}

impl TryFrom<StoredEntry> for Bytes {
    type Error = ciborium::ser::Error<std::io::Error>;

    fn try_from(storage_entry: StoredEntry) -> Result<Self, Self::Error> {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&storage_entry, &mut bytes)?;

        Ok(Self::from(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stored_entry_serialization() {
        let entry = StoredEntry::File {
            content: Bytes::from("test content"),
            metadata: FsMetadata {
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                mtime: None,
                atime: None,
                birthtime: None,
                ctime: None,
            },
        };

        // Serialize to bytes
        let bytes: Bytes = entry.clone().try_into().unwrap();

        // Deserialize back
        let decoded: StoredEntry = bytes.try_into().unwrap();

        match (entry, decoded) {
            (
                StoredEntry::File {
                    content: c1,
                    metadata: m1,
                },
                StoredEntry::File {
                    content: c2,
                    metadata: m2,
                },
            ) => {
                assert_eq!(c1, c2);
                assert_eq!(m1.mode, m2.mode);
                assert_eq!(m1.uid, m2.uid);
                assert_eq!(m1.gid, m2.gid);
            }
            _ => panic!("Expected File entries"),
        }
    }
}
