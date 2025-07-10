//! S3 One Zone storage adaptor with VSOCK-based WAL for TEE
//!
//! This module implements a cloud-native storage adaptor that runs within a Nitro Enclave
//! and uses S3 One Zone for persistent storage with a VSOCK-based encrypted WAL on the host
//! machine for durability.

mod batching;
mod cache;
mod config;
mod encryption;
mod iterator;
mod keys;
mod metrics;
mod s3_storage;
mod wal_client;

pub use config::{BatchConfig, CacheConfig, S3Config, S3StorageConfig, WalConfig};
pub use s3_storage::S3StorageAdaptor;
pub use wal_client::{WalClient, WalEntry, WalEntryId};

#[cfg(test)]
mod tests;
