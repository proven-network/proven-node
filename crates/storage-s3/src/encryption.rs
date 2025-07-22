//! Encryption support for S3 storage

use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, AeadCore, KeyInit, OsRng},
};
use base64::{Engine as _, engine::general_purpose};
use bytes::{Bytes, BytesMut};
use miniz_oxide::deflate::compress_to_vec;
use miniz_oxide::inflate::decompress_to_vec;
use proven_logger::{debug, trace};
use proven_storage::{StorageError, StorageResult};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::config::EncryptionConfig;

/// Encryption handler for S3 storage
pub struct EncryptionHandler {
    /// Configuration
    config: EncryptionConfig,
    /// S3 cipher
    s3_cipher: Arc<RwLock<Aes256Gcm>>,
    /// WAL cipher
    wal_cipher: Arc<RwLock<Aes256Gcm>>,
    /// Compression threshold
    compression_threshold: usize,
}

impl EncryptionHandler {
    /// Create a new encryption handler
    pub fn new(config: EncryptionConfig, compression_threshold: usize) -> StorageResult<Self> {
        // Decode keys
        let s3_key = general_purpose::STANDARD
            .decode(&config.s3_key)
            .map_err(|e| StorageError::InvalidValue(format!("Invalid S3 key: {e}")))?;
        let wal_key = general_purpose::STANDARD
            .decode(&config.wal_key)
            .map_err(|e| StorageError::InvalidValue(format!("Invalid WAL key: {e}")))?;

        // Validate key length (32 bytes for AES-256)
        if s3_key.len() != 32 {
            return Err(StorageError::InvalidValue(
                "S3 key must be 32 bytes".to_string(),
            ));
        }
        if wal_key.len() != 32 {
            return Err(StorageError::InvalidValue(
                "WAL key must be 32 bytes".to_string(),
            ));
        }

        // Create ciphers
        let s3_cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&s3_key));
        let wal_cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&wal_key));

        Ok(Self {
            config,
            s3_cipher: Arc::new(RwLock::new(s3_cipher)),
            wal_cipher: Arc::new(RwLock::new(wal_cipher)),
            compression_threshold,
        })
    }

    /// Encrypt data for S3 storage
    pub async fn encrypt_for_s3(&self, data: &[u8]) -> StorageResult<Bytes> {
        self.encrypt(data, &self.s3_cipher).await
    }

    /// Decrypt data from S3 storage
    pub async fn decrypt_from_s3(&self, data: &[u8]) -> StorageResult<Bytes> {
        self.decrypt(data, &self.s3_cipher).await
    }

    /// Encrypt data for WAL storage
    pub async fn encrypt_for_wal(&self, data: &[u8]) -> StorageResult<Vec<u8>> {
        let result = self.encrypt(data, &self.wal_cipher).await?;
        Ok(result.to_vec())
    }

    /// Decrypt data from WAL storage
    pub async fn decrypt_from_wal(&self, data: &[u8]) -> StorageResult<Bytes> {
        self.decrypt(data, &self.wal_cipher).await
    }

    /// Internal encryption method
    async fn encrypt(&self, data: &[u8], cipher: &Arc<RwLock<Aes256Gcm>>) -> StorageResult<Bytes> {
        let mut output = BytesMut::new();

        // Check if we should compress
        let should_compress = data.len() >= self.compression_threshold;

        // Write header byte
        let header = if should_compress { 0x01 } else { 0x00 };
        output.extend_from_slice(&[header]);

        // Compress if needed
        let payload = if should_compress {
            trace!("Compressing {} bytes", data.len());
            let compressed = compress_to_vec(data, 6); // Level 6 compression
            trace!("Compressed to {} bytes", compressed.len());
            compressed
        } else {
            data.to_vec()
        };

        // Generate nonce (12 bytes for GCM)
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        output.extend_from_slice(&nonce);

        // Encrypt
        let cipher_guard = cipher.read().await;
        let ciphertext = cipher_guard
            .encrypt(&nonce, payload.as_ref())
            .map_err(|e| StorageError::Backend(format!("Encryption failed: {e}")))?;

        output.extend_from_slice(&ciphertext);

        Ok(output.freeze())
    }

    /// Internal decryption method
    async fn decrypt(&self, data: &[u8], cipher: &Arc<RwLock<Aes256Gcm>>) -> StorageResult<Bytes> {
        if data.len() < 13 {
            // 1 byte header + 12 byte nonce
            return Err(StorageError::InvalidValue(
                "Encrypted data too short".to_string(),
            ));
        }

        // Read header
        let header = data[0];
        let compressed = (header & 0x01) != 0;

        // Extract nonce
        let nonce = Nonce::from_slice(&data[1..13]);

        // Decrypt
        let cipher_guard = cipher.read().await;
        let plaintext = cipher_guard
            .decrypt(nonce, &data[13..])
            .map_err(|e| StorageError::Backend(format!("Decryption failed: {e}")))?;

        // Decompress if needed
        let result = if compressed {
            trace!("Decompressing {} bytes", plaintext.len());
            let decompressed = decompress_to_vec(&plaintext)
                .map_err(|e| StorageError::Backend(format!("Decompression failed: {e}")))?;
            trace!("Decompressed to {} bytes", decompressed.len());
            Bytes::from(decompressed)
        } else {
            Bytes::from(plaintext)
        };

        Ok(result)
    }

    /// Rotate encryption keys
    pub async fn rotate_keys(&self, new_s3_key: &str, new_wal_key: &str) -> StorageResult<()> {
        if !self.config.enable_rotation {
            return Err(StorageError::NotSupported(
                "Key rotation not enabled".to_string(),
            ));
        }

        // Decode new keys
        let s3_key = general_purpose::STANDARD
            .decode(new_s3_key)
            .map_err(|e| StorageError::InvalidValue(format!("Invalid S3 key: {e}")))?;
        let wal_key = general_purpose::STANDARD
            .decode(new_wal_key)
            .map_err(|e| StorageError::InvalidValue(format!("Invalid WAL key: {e}")))?;

        // Validate key length
        if s3_key.len() != 32 || wal_key.len() != 32 {
            return Err(StorageError::InvalidValue(
                "Keys must be 32 bytes".to_string(),
            ));
        }

        // Update ciphers
        let new_s3_cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&s3_key));
        let new_wal_cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&wal_key));

        *self.s3_cipher.write().await = new_s3_cipher;
        *self.wal_cipher.write().await = new_wal_cipher;

        debug!("Encryption keys rotated");

        Ok(())
    }
}

/// Generate a new encryption key
pub fn generate_key() -> String {
    let key = Aes256Gcm::generate_key(&mut OsRng);
    general_purpose::STANDARD.encode(key.as_slice())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_encryption_decryption() {
        let config = EncryptionConfig {
            s3_key: generate_key(),
            wal_key: generate_key(),
            enable_rotation: false,
            rotation_interval: std::time::Duration::from_secs(86400),
        };

        let handler = EncryptionHandler::new(config, 100).unwrap();

        // Test small data (no compression)
        let small_data = b"Hello, World!";
        let encrypted = handler.encrypt_for_s3(small_data).await.unwrap();
        let decrypted = handler.decrypt_from_s3(&encrypted).await.unwrap();
        assert_eq!(&decrypted[..], small_data);

        // Test large data (with compression)
        let large_data = vec![b'A'; 1000];
        let encrypted = handler.encrypt_for_s3(&large_data).await.unwrap();
        let decrypted = handler.decrypt_from_s3(&encrypted).await.unwrap();
        assert_eq!(&decrypted[..], &large_data[..]);

        // Compressed should be smaller than original + encryption overhead
        assert!(encrypted.len() < large_data.len());
    }

    #[tokio::test]
    async fn test_key_rotation() {
        let config = EncryptionConfig {
            s3_key: generate_key(),
            wal_key: generate_key(),
            enable_rotation: true,
            rotation_interval: std::time::Duration::from_secs(86400),
        };

        let handler = EncryptionHandler::new(config, 100).unwrap();

        // Encrypt with old key
        let data = b"Test data";
        let encrypted = handler.encrypt_for_s3(data).await.unwrap();

        // Rotate keys
        let new_s3_key = generate_key();
        let new_wal_key = generate_key();
        handler
            .rotate_keys(&new_s3_key, &new_wal_key)
            .await
            .unwrap();

        // Old encrypted data cannot be decrypted with new key
        assert!(handler.decrypt_from_s3(&encrypted).await.is_err());

        // But we can encrypt/decrypt with new key
        let encrypted2 = handler.encrypt_for_s3(data).await.unwrap();
        let decrypted2 = handler.decrypt_from_s3(&encrypted2).await.unwrap();
        assert_eq!(&decrypted2[..], data);
    }
}
