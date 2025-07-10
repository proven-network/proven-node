//! Storage backend implementations

pub mod memory;
pub mod rocksdb;

#[cfg(feature = "s3")]
pub mod s3;

pub use memory::MemoryStorage;
pub use rocksdb::{RocksDBAdaptorConfig, RocksDBStorage};

#[cfg(feature = "s3")]
pub use s3::{S3StorageAdaptor, S3StorageConfig};
