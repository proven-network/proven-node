use super::file::File;
use super::metadata::FsMetadata;
use super::stored_entry::StoredEntry;

use bytes::BytesMut;
use proven_store::Store;

/// Represents a file system entry.
#[derive(Debug)]
pub enum Entry<S>
where
    S: Store<
            StoredEntry,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
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

#[allow(dead_code)]
impl<S> Entry<S>
where
    S: Store<
            StoredEntry,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
{
    pub(crate) fn content(&self) -> Option<BytesMut> {
        match self {
            Self::File(file) => Some(file.content()),
            _ => None,
        }
    }

    pub(crate) fn with_content_mut<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&mut BytesMut) -> R,
    {
        match self {
            Self::File(file) => Some(file.with_content_mut(f)),
            _ => None,
        }
    }

    pub(crate) fn metadata(&self) -> FsMetadata {
        match self {
            Self::File(file) => file.metadata(),
            Self::Directory { metadata } | Self::Symlink { metadata, .. } => metadata.clone(),
        }
    }

    pub(crate) fn with_metadata_mut<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&mut FsMetadata) -> R,
    {
        match self {
            Self::File(file) => Some(file.with_metadata_mut(f)),
            _ => None,
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
