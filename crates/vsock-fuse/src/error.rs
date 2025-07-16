//! Error types for VSOCK-FUSE

use std::path::PathBuf;
use thiserror::Error;

/// Main error type for VSOCK-FUSE operations
#[derive(Debug, Error)]
pub enum VsockFuseError {
    /// Encryption-related errors
    #[error("Encryption error: {0}")]
    Encryption(#[from] EncryptionError),

    /// RPC communication errors
    #[error("RPC communication error: {0}")]
    Rpc(#[from] proven_vsock_rpc::Error),

    /// Storage-related errors
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    /// File not found
    #[error("File not found: {path}")]
    NotFound { path: PathBuf },

    /// Permission denied
    #[error("Permission denied: {path}")]
    PermissionDenied { path: PathBuf },

    /// File already exists
    #[error("File already exists: {path}")]
    AlreadyExists { path: PathBuf },

    /// Not a directory
    #[error("Not a directory: {path}")]
    NotDirectory { path: PathBuf },

    /// Is a directory
    #[error("Is a directory: {path}")]
    IsDirectory { path: PathBuf },

    /// Directory not empty
    #[error("Directory not empty: {path}")]
    NotEmpty { path: PathBuf },

    /// Invalid argument
    #[error("Invalid argument: {message}")]
    InvalidArgument { message: String },

    /// I/O error
    #[error("I/O error: {message}")]
    Io {
        message: String,
        #[source]
        source: Option<std::io::Error>,
    },

    /// No space left
    #[error("No space left: required {required} bytes, available {available} bytes")]
    NoSpace {
        path: PathBuf,
        required: u64,
        available: u64,
    },

    /// Quota exceeded
    #[error("Quota exceeded: {path}, limit {limit} bytes")]
    QuotaExceeded { path: PathBuf, limit: u64 },

    /// File is locked
    #[error("File is locked: {path}, holder: {holder}")]
    Locked { path: PathBuf, holder: String },

    /// Data integrity error
    #[error("Data integrity error: checksum mismatch")]
    IntegrityError,

    /// Configuration error
    #[error("Configuration error: {message}")]
    Configuration { message: String },

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),

    /// Blob not found
    #[error("Blob not found: {id:?}")]
    BlobNotFound { id: crate::BlobId },

    /// Not implemented
    #[error("Feature not implemented: {feature}")]
    NotImplemented { feature: String },

    /// Directory already exists
    #[error("Directory already exists: {path}")]
    DirectoryExists { path: PathBuf },
}

/// Encryption-specific errors
#[derive(Debug, Error)]
pub enum EncryptionError {
    /// Key derivation failed
    #[error("Key derivation failed")]
    KeyDerivation,

    /// Encryption operation failed
    #[error("Encryption failed")]
    EncryptionFailed,

    /// Decryption operation failed
    #[error("Decryption failed")]
    DecryptionFailed,

    /// MAC verification failed
    #[error("MAC verification failed")]
    MacVerificationFailed,

    /// Invalid nonce
    #[error("Invalid nonce")]
    InvalidNonce,

    /// Invalid key size
    #[error("Invalid key size: expected {expected}, got {actual}")]
    InvalidKeySize { expected: usize, actual: usize },
}

/// Storage-specific errors
#[derive(Debug, Error)]
pub enum StorageError {
    /// Blob not found
    #[error("Blob not found: {blob_id:?}")]
    BlobNotFound { blob_id: crate::BlobId },

    /// Tier unavailable
    #[error("Storage tier unavailable: {tier:?}")]
    TierUnavailable { tier: crate::StorageTier },

    /// Migration in progress
    #[error("Migration in progress for: {path}")]
    MigrationInProgress { path: PathBuf },

    /// S3 error
    #[error("S3 error: {message}")]
    S3Error { message: String },

    /// Local storage error
    #[error("Local storage error: {message}")]
    LocalStorageError { message: String },
}

/// Result type alias for VSOCK-FUSE operations
pub type Result<T> = std::result::Result<T, VsockFuseError>;

/// Convert from std::io::Error
impl From<std::io::Error> for VsockFuseError {
    fn from(err: std::io::Error) -> Self {
        Self::Io {
            message: err.to_string(),
            source: Some(err),
        }
    }
}

/// Convert to POSIX error codes for FUSE
impl VsockFuseError {
    /// Convert to errno value for FUSE
    pub fn to_errno(&self) -> libc::c_int {
        match self {
            Self::NotFound { .. } => libc::ENOENT,
            Self::PermissionDenied { .. } => libc::EACCES,
            Self::AlreadyExists { .. } => libc::EEXIST,
            Self::NotDirectory { .. } => libc::ENOTDIR,
            Self::IsDirectory { .. } => libc::EISDIR,
            Self::NotEmpty { .. } => libc::ENOTEMPTY,
            Self::InvalidArgument { .. } => libc::EINVAL,
            Self::Io { .. } => libc::EIO,
            Self::NoSpace { .. } => libc::ENOSPC,
            Self::QuotaExceeded { .. } => libc::EDQUOT,
            Self::Locked { .. } => libc::EAGAIN,
            Self::IntegrityError => libc::EIO,
            Self::Encryption(_) => libc::EIO,
            Self::Rpc(_) => libc::EIO,
            Self::Storage(_) => libc::EIO,
            Self::Configuration { .. } => libc::EINVAL,
            Self::Internal(_) => libc::EIO,
            Self::BlobNotFound { .. } => libc::ENOENT,
            Self::NotImplemented { .. } => libc::ENOSYS,
            Self::DirectoryExists { .. } => libc::EEXIST,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_to_errno() {
        let err = VsockFuseError::NotFound {
            path: PathBuf::from("/test"),
        };
        assert_eq!(err.to_errno(), libc::ENOENT);

        let err = VsockFuseError::PermissionDenied {
            path: PathBuf::from("/test"),
        };
        assert_eq!(err.to_errno(), libc::EACCES);
    }
}
