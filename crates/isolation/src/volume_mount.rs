use std::path::{Path, PathBuf};

/// Represents a volume mount from the host to the container
#[derive(Debug, Clone)]
pub struct VolumeMount {
    /// The path on the host system (resolved from symlinks)
    pub host_path: PathBuf,
    /// The path inside the container where the volume will be mounted
    pub container_path: PathBuf,
    /// Whether the mount should be read-only
    pub read_only: bool,
}

impl VolumeMount {
    /// Creates a new volume mount
    ///
    /// # Note
    ///
    /// The `host_path` will be canonicalized to resolve any symlinks.
    /// If canonicalization fails, the original path will be used.
    #[must_use]
    pub fn new<P: AsRef<Path>>(host_path: P, container_path: P) -> Self {
        let resolved_host_path = std::fs::canonicalize(host_path.as_ref())
            .unwrap_or_else(|_| host_path.as_ref().to_path_buf());

        Self {
            host_path: resolved_host_path,
            container_path: container_path.as_ref().to_path_buf(),
            read_only: false,
        }
    }

    /// Creates a new read-only volume mount
    ///
    /// # Note
    ///
    /// The `host_path` will be canonicalized to resolve any symlinks.
    /// If canonicalization fails, the original path will be used.
    #[must_use]
    pub fn new_read_only<P: AsRef<Path>>(host_path: P, container_path: P) -> Self {
        let resolved_host_path = std::fs::canonicalize(host_path.as_ref())
            .unwrap_or_else(|_| host_path.as_ref().to_path_buf());

        Self {
            host_path: resolved_host_path,
            container_path: container_path.as_ref().to_path_buf(),
            read_only: true,
        }
    }
}
