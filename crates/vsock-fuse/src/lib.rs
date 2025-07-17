//! # VSOCK-FUSE
//!
//! An encrypted filesystem solution for AWS Nitro Enclaves that provides POSIX-compliant
//! persistent storage through a secure, tiered storage architecture.
//!
//! ## Overview
//!
//! VSOCK-FUSE implements end-to-end encryption where all data is encrypted with AES-256-GCM
//! within the enclave before transmission to the host. The host never has access to plaintext
//! data or encryption keys.
//!
//! ## Architecture
//!
//! - **FUSE Layer**: Provides POSIX-compliant filesystem interface
//! - **Encryption Layer**: AES-256-GCM encryption with SHA256-based key derivation (optimized for performance)
//! - **Metadata Store**: Encrypted file and directory metadata management
//! - **RPC Communication**: Secure blob-based operations over VSOCK
//! - **Tiered Storage**: Intelligent data placement between NVMe and S3
//!
//! ## Usage
//!
//! ### Enclave Side
//! ```no_run
//! use proven_vsock_fuse::config::Config;
//! use proven_vsock_fuse::enclave::{EnclaveService, EnclaveServiceBuilder};
//! use proven_vsock_fuse::encryption::MasterKey;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Load configuration
//! let config = Config::from_file(&std::path::PathBuf::from("config.toml"))?;
//!
//! // Create enclave service
//! let service = EnclaveServiceBuilder::new()
//!     .config(config)
//!     .derive_master_key(b"attestation_data", Some(b"user_seed"))?
//!     .build()?;
//!
//! // Mount the filesystem
//! service.mount(std::path::Path::new("/mnt/secure"))?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Host Side
//! ```no_run
//! use proven_vsock_fuse::config::Config;
//! use proven_vsock_fuse::host::{HostService, HostServiceBuilder};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create host service
//! let mut service = HostServiceBuilder::new()
//!     .config_from_file(&std::path::Path::new("config.toml"))?
//!     .build()?;
//!
//! // Start the service
//! service.start().await?;
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};

pub mod config;
pub mod database;
pub mod encryption;
pub mod error;
pub mod fuse;
pub mod fuse_async;
pub mod metadata;
pub mod rpc;
pub mod storage;

// Separate modules for enclave and host
#[cfg(feature = "enclave")]
pub mod enclave;

#[cfg(feature = "host")]
pub mod host;

/// Unique identifier for a file
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileId(pub [u8; 32]);

impl FileId {
    /// Create a new random FileId
    pub fn new() -> Self {
        let mut id = [0u8; 32];
        rand::Rng::fill(&mut rand::thread_rng(), &mut id);
        Self(id)
    }

    /// Create a FileId from bytes
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Get the bytes of the FileId
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Default for FileId {
    fn default() -> Self {
        Self::new()
    }
}

/// Unique identifier for a blob in storage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlobId(pub [u8; 32]);

impl BlobId {
    /// Create a new random BlobId
    pub fn new() -> Self {
        let mut id = [0u8; 32];
        rand::Rng::fill(&mut rand::thread_rng(), &mut id);
        Self(id)
    }

    /// Create a BlobId from a FileId and blob type
    pub fn from_file_id(file_id: &FileId, blob_type: BlobType) -> Self {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(file_id.as_bytes());
        hasher.update([blob_type as u8]);
        let result = hasher.finalize();
        let mut id = [0u8; 32];
        id.copy_from_slice(&result);
        Self(id)
    }
}

impl Default for BlobId {
    fn default() -> Self {
        Self::new()
    }
}

/// Unique identifier for a directory
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DirectoryId(pub FileId);

impl DirectoryId {
    /// Create a new random DirectoryId
    pub fn new() -> Self {
        Self(FileId::new())
    }

    /// Get the underlying FileId
    pub fn as_file_id(&self) -> &FileId {
        &self.0
    }
}

/// Type of blob in storage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum BlobType {
    /// File metadata blob
    Metadata = 0,
    /// File data blob
    Data = 1,
    /// Directory metadata blob
    Directory = 2,
}

/// Type of file in the filesystem
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileType {
    /// Regular file
    Regular,
    /// Directory
    Directory,
    /// Symbolic link
    Symlink,
}

/// Location of a data block
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockLocation {
    /// Block number within the file
    pub block_num: u64,
    /// Blob ID where the block is stored
    pub blob_id: BlobId,
    /// Offset within the blob
    pub offset: u64,
    /// Size of the encrypted block
    pub encrypted_size: usize,
}

/// File metadata structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    /// Unique file identifier
    pub file_id: FileId,
    /// Parent directory ID
    pub parent_id: Option<DirectoryId>,
    /// Encrypted file name
    pub encrypted_name: Vec<u8>,
    /// File size in bytes
    pub size: u64,
    /// List of block locations
    pub blocks: Vec<BlockLocation>,
    /// Creation timestamp
    pub created_at: std::time::SystemTime,
    /// Last modification timestamp
    pub modified_at: std::time::SystemTime,
    /// Last access timestamp
    pub accessed_at: std::time::SystemTime,
    /// Unix permissions
    pub permissions: u32,
    /// File type
    pub file_type: FileType,
    /// Number of hard links
    pub nlink: u32,
    /// Owner user ID
    pub uid: u32,
    /// Owner group ID
    pub gid: u32,
}

/// Storage tier for data placement
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageTier {
    /// Hot tier (local NVMe)
    Hot,
    /// Cold tier (S3)
    Cold,
    /// Data exists in both tiers
    Both,
}

/// Hint for storage tier placement
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TierHint {
    /// Prefer hot tier placement
    PreferHot,
    /// Prefer cold tier placement
    PreferCold,
    /// Let system decide based on policy
    Auto,
}

/// Access type for tracking patterns
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessType {
    /// Sequential read access
    SequentialRead,
    /// Random read access
    RandomRead,
    /// Write access
    Write,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_id_creation() {
        let id1 = FileId::new();
        let id2 = FileId::new();
        assert_ne!(id1, id2);
        assert_eq!(id1.as_bytes().len(), 32);
    }

    #[test]
    fn test_blob_id_from_file_id() {
        let file_id = FileId::new();
        let blob_id1 = BlobId::from_file_id(&file_id, BlobType::Metadata);
        let blob_id2 = BlobId::from_file_id(&file_id, BlobType::Metadata);
        assert_eq!(blob_id1, blob_id2);

        let blob_id3 = BlobId::from_file_id(&file_id, BlobType::Data);
        assert_ne!(blob_id1, blob_id3);
    }
}
