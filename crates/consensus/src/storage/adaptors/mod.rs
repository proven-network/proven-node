//! Storage backend implementations

pub mod memory;
pub mod rocksdb;

pub use memory::MemoryStorage;
pub use rocksdb::{RocksDBAdaptorConfig, RocksDBStorage};

// Future modules:
// pub mod s3;
