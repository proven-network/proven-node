//! Encryption module for S3 storage adaptor

use crate::storage_backends::types::{StorageError, StorageResult};
use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit, OsRng},
};
use bytes::Bytes;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

/// Encrypted data with metadata
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EncryptedData {
    /// The encrypted ciphertext
    pub ciphertext: Vec<u8>,
    /// The nonce used for encryption
    pub nonce: [u8; 12],
    /// Key version for rotation support
    pub key_version: u32,
}

/// Key rotation support
#[derive(Clone)]
struct RotatingKey {
    current_version: u32,
    current_key: Key<Aes256Gcm>,
    previous_keys: Vec<(u32, Key<Aes256Gcm>)>,
}

impl RotatingKey {
    fn new() -> Self {
        let mut key_bytes = [0u8; 32];
        OsRng.fill_bytes(&mut key_bytes);

        Self {
            current_version: 1,
            current_key: *Key::<Aes256Gcm>::from_slice(&key_bytes),
            previous_keys: Vec::new(),
        }
    }

    fn rotate(&mut self) {
        let mut new_key_bytes = [0u8; 32];
        OsRng.fill_bytes(&mut new_key_bytes);

        self.previous_keys
            .push((self.current_version, self.current_key));
        self.current_version += 1;
        self.current_key = *Key::<Aes256Gcm>::from_slice(&new_key_bytes);

        // Keep only last 3 versions for decryption
        if self.previous_keys.len() > 3 {
            self.previous_keys.remove(0);
        }
    }

    fn get_key(&self, version: u32) -> Option<&Key<Aes256Gcm>> {
        if version == self.current_version {
            Some(&self.current_key)
        } else {
            self.previous_keys
                .iter()
                .find(|(v, _)| *v == version)
                .map(|(_, k)| k)
        }
    }
}

/// Encryption manager for S3 and WAL data
pub struct EncryptionManager {
    /// Master key (sealed to enclave)
    #[allow(dead_code)]
    master_key: [u8; 32],
    /// WAL encryption key
    wal_key: Arc<RwLock<RotatingKey>>,
    /// S3 encryption key
    s3_key: Arc<RwLock<RotatingKey>>,
}

impl EncryptionManager {
    /// Create a new encryption manager
    pub async fn new() -> StorageResult<Self> {
        // In production, this would use enclave sealing
        let mut master_key = [0u8; 32];
        OsRng.fill_bytes(&mut master_key);

        debug!("Initializing encryption manager");

        Ok(Self {
            master_key,
            wal_key: Arc::new(RwLock::new(RotatingKey::new())),
            s3_key: Arc::new(RwLock::new(RotatingKey::new())),
        })
    }

    /// Encrypt data for WAL storage
    pub async fn encrypt_for_wal(&self, data: &[u8]) -> StorageResult<Bytes> {
        let key_guard = self.wal_key.read().await;
        let cipher = Aes256Gcm::new(&key_guard.current_key);

        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher
            .encrypt(nonce, data)
            .map_err(|e| StorageError::EncryptionError(format!("WAL encryption failed: {e}")))?;

        let encrypted = EncryptedData {
            ciphertext,
            nonce: nonce_bytes,
            key_version: key_guard.current_version,
        };

        let mut buf = Vec::new();
        ciborium::into_writer(&encrypted, &mut buf).map_err(|e| {
            StorageError::InvalidValue(format!("Failed to serialize encrypted data: {e}"))
        })?;
        Ok(Bytes::from(buf))
    }

    /// Decrypt data from WAL storage
    pub async fn decrypt_from_wal(&self, encrypted_bytes: &[u8]) -> StorageResult<Bytes> {
        let encrypted: EncryptedData = ciborium::from_reader(encrypted_bytes).map_err(|e| {
            StorageError::InvalidValue(format!("Failed to deserialize encrypted data: {e}"))
        })?;

        let key_guard = self.wal_key.read().await;
        let key = key_guard.get_key(encrypted.key_version).ok_or_else(|| {
            StorageError::EncryptionError(format!(
                "Key version {} not found",
                encrypted.key_version
            ))
        })?;

        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&encrypted.nonce);

        let plaintext = cipher
            .decrypt(nonce, encrypted.ciphertext.as_ref())
            .map_err(|e| StorageError::EncryptionError(format!("WAL decryption failed: {e}")))?;

        Ok(Bytes::from(plaintext))
    }

    /// Encrypt data for S3 storage
    pub async fn encrypt_for_s3(&self, data: &[u8]) -> StorageResult<Bytes> {
        let key_guard = self.s3_key.read().await;
        let cipher = Aes256Gcm::new(&key_guard.current_key);

        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        // Optionally compress before encryption
        let compressed = if data.len() > 1024 {
            let compressed = miniz_oxide::deflate::compress_to_vec(data, 6);
            if compressed.len() < data.len() {
                compressed
            } else {
                data.to_vec()
            }
        } else {
            data.to_vec()
        };

        let ciphertext = cipher
            .encrypt(nonce, compressed.as_ref())
            .map_err(|e| StorageError::EncryptionError(format!("S3 encryption failed: {e}")))?;

        let encrypted = EncryptedData {
            ciphertext,
            nonce: nonce_bytes,
            key_version: key_guard.current_version,
        };

        let mut buf = Vec::new();
        ciborium::into_writer(&encrypted, &mut buf).map_err(|e| {
            StorageError::InvalidValue(format!("Failed to serialize encrypted data: {e}"))
        })?;
        Ok(Bytes::from(buf))
    }

    /// Decrypt data from S3 storage
    pub async fn decrypt_from_s3(&self, encrypted_bytes: &[u8]) -> StorageResult<Bytes> {
        let encrypted: EncryptedData = ciborium::from_reader(encrypted_bytes).map_err(|e| {
            StorageError::InvalidValue(format!("Failed to deserialize encrypted data: {e}"))
        })?;

        let key_guard = self.s3_key.read().await;
        let key = key_guard.get_key(encrypted.key_version).ok_or_else(|| {
            StorageError::EncryptionError(format!(
                "Key version {} not found",
                encrypted.key_version
            ))
        })?;

        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&encrypted.nonce);

        let compressed = cipher
            .decrypt(nonce, encrypted.ciphertext.as_ref())
            .map_err(|e| StorageError::EncryptionError(format!("S3 decryption failed: {e}")))?;

        // Try to decompress
        let plaintext = match miniz_oxide::inflate::decompress_to_vec(&compressed) {
            Ok(decompressed) => decompressed,
            Err(_) => compressed, // Not compressed, use as-is
        };

        Ok(Bytes::from(plaintext))
    }

    /// Rotate WAL encryption key
    pub async fn rotate_wal_key(&self) -> StorageResult<()> {
        let mut key_guard = self.wal_key.write().await;
        key_guard.rotate();
        debug!(
            "Rotated WAL encryption key to version {}",
            key_guard.current_version
        );
        Ok(())
    }

    /// Rotate S3 encryption key
    pub async fn rotate_s3_key(&self) -> StorageResult<()> {
        let mut key_guard = self.s3_key.write().await;
        key_guard.rotate();
        debug!(
            "Rotated S3 encryption key to version {}",
            key_guard.current_version
        );
        Ok(())
    }
}
