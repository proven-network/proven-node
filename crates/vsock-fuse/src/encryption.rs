//! Encryption layer for VSOCK-FUSE
//!
//! This module implements AES-256-GCM encryption with fast SHA256-based key derivation.
//! All encryption operations happen within the enclave boundary.
//!
//! Key derivation uses SHA256 instead of Argon2 for performance:
//! - File keys are derived once and cached
//! - Each block uses the file key with a unique nonce
//! - This provides both security and high performance for filesystem operations

use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, AeadCore, KeyInit, OsRng},
};
use dashmap::DashMap;
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::{
    FileId,
    error::{EncryptionError, Result, VsockFuseError},
};

/// Size of AES-256 key in bytes
const KEY_SIZE: usize = 32;

/// Size of AES-GCM nonce in bytes
const NONCE_SIZE: usize = 12;

/// Size of AES-GCM authentication tag in bytes
const TAG_SIZE: usize = 16;

/// Master key for the enclave
#[derive(Clone)]
pub struct MasterKey([u8; KEY_SIZE]);

impl MasterKey {
    /// Create a master key from bytes
    pub fn from_bytes(bytes: [u8; KEY_SIZE]) -> Self {
        Self(bytes)
    }

    /// Get the bytes of the master key
    pub fn as_bytes(&self) -> &[u8; KEY_SIZE] {
        &self.0
    }

    /// Generate a new random master key
    pub fn generate() -> Self {
        let mut key = [0u8; KEY_SIZE];
        rand::thread_rng().fill(&mut key);
        Self(key)
    }
}

/// File-specific encryption key
#[derive(Clone)]
pub struct FileKey([u8; KEY_SIZE]);

impl FileKey {
    /// Create a file key from bytes
    pub fn from_bytes(bytes: [u8; KEY_SIZE]) -> Self {
        Self(bytes)
    }

    /// Get the bytes of the file key
    pub fn as_bytes(&self) -> &[u8; KEY_SIZE] {
        &self.0
    }
}

/// Encrypted block structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedBlock {
    /// Block number within the file
    pub block_num: u64,
    /// Nonce used for encryption
    pub nonce: [u8; NONCE_SIZE],
    /// Encrypted data with appended MAC
    pub ciphertext: Vec<u8>,
}

/// Encryption layer implementation
pub struct EncryptionLayer {
    /// Master key for the enclave
    master_key: MasterKey,
    /// Block size for encryption
    block_size: usize,
    /// Cache of derived file keys
    file_key_cache: DashMap<FileId, FileKey>,
}

impl EncryptionLayer {
    /// Create a new encryption layer
    pub fn new(master_key: MasterKey, block_size: usize) -> Result<Self> {
        Ok(Self {
            master_key,
            block_size,
            file_key_cache: DashMap::new(),
        })
    }

    /// Derive a unique key for a file (with caching)
    pub fn derive_file_key(&self, file_id: &FileId) -> Result<FileKey> {
        // Check cache first
        if let Some(cached_key) = self.file_key_cache.get(file_id) {
            return Ok(cached_key.clone());
        }

        // Not in cache, derive it using SHA256 (fast)
        // This is secure because:
        // 1. The master key is already cryptographically strong (protected by enclave)
        // 2. File IDs are unique
        // 3. We use a proper domain separator
        let mut hasher = Sha256::new();
        hasher.update(self.master_key.as_bytes());
        hasher.update(b"FILE_KEY_DERIVATION");
        hasher.update(file_id.as_bytes());

        let result = hasher.finalize();
        let mut file_key_bytes = [0u8; KEY_SIZE];
        file_key_bytes.copy_from_slice(&result[..KEY_SIZE]);

        let file_key = FileKey(file_key_bytes);

        // Cache the derived key
        self.file_key_cache.insert(*file_id, file_key.clone());

        Ok(file_key)
    }

    /// Encrypt a block of data (returns EncryptedBlock structure)
    pub fn encrypt_block_structured(
        &self,
        file_id: &FileId,
        block_num: u64,
        plaintext: &[u8],
    ) -> Result<EncryptedBlock> {
        let file_key = self.derive_file_key(file_id)?;

        // Generate nonce with block number to ensure uniqueness
        let mut nonce_bytes = [0u8; NONCE_SIZE];
        nonce_bytes[..8].copy_from_slice(&block_num.to_le_bytes());
        rand::thread_rng().fill(&mut nonce_bytes[8..]);

        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(file_key.as_bytes()));
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher
            .encrypt(nonce, plaintext)
            .map_err(|_| EncryptionError::EncryptionFailed)?;

        Ok(EncryptedBlock {
            block_num,
            nonce: nonce_bytes,
            ciphertext,
        })
    }

    /// Decrypt a block of data (from EncryptedBlock structure)
    pub fn decrypt_block_structured(
        &self,
        file_id: &FileId,
        block: &EncryptedBlock,
    ) -> Result<Vec<u8>> {
        let file_key = self.derive_file_key(file_id)?;

        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(file_key.as_bytes()));
        let nonce = Nonce::from_slice(&block.nonce);

        cipher
            .decrypt(nonce, block.ciphertext.as_ref())
            .map_err(|_| EncryptionError::DecryptionFailed.into())
    }

    /// Encrypt metadata
    pub fn encrypt_metadata(&self, metadata: &[u8]) -> Result<Vec<u8>> {
        // Use a fixed file ID for metadata encryption
        let metadata_id = FileId::from_bytes([0xFF; 32]);
        let metadata_key = self.derive_file_key(&metadata_id)?;

        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(metadata_key.as_bytes()));
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

        let mut ciphertext = cipher
            .encrypt(&nonce, metadata)
            .map_err(|_| EncryptionError::EncryptionFailed)?;

        // Prepend nonce to ciphertext
        let mut result = nonce.to_vec();
        result.append(&mut ciphertext);

        Ok(result)
    }

    /// Decrypt metadata
    pub fn decrypt_metadata(&self, encrypted: &[u8]) -> Result<Vec<u8>> {
        if encrypted.len() < NONCE_SIZE {
            return Err(EncryptionError::InvalidNonce.into());
        }

        let (nonce_bytes, ciphertext) = encrypted.split_at(NONCE_SIZE);
        let nonce = Nonce::from_slice(nonce_bytes);

        // Use the same fixed file ID for metadata
        let metadata_id = FileId::from_bytes([0xFF; 32]);
        let metadata_key = self.derive_file_key(&metadata_id)?;

        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(metadata_key.as_bytes()));

        cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| EncryptionError::DecryptionFailed.into())
    }

    /// Encrypt a filename
    pub fn encrypt_filename(&self, dir_id: &FileId, filename: &str) -> Result<Vec<u8>> {
        // Derive a key specific to the directory
        let dir_key = self.derive_file_key(dir_id)?;

        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(dir_key.as_bytes()));
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

        let mut ciphertext = cipher
            .encrypt(&nonce, filename.as_bytes())
            .map_err(|_| EncryptionError::EncryptionFailed)?;

        // Prepend nonce
        let mut result = nonce.to_vec();
        result.append(&mut ciphertext);

        Ok(result)
    }

    /// Decrypt a filename
    pub fn decrypt_filename(&self, dir_id: &FileId, encrypted: &[u8]) -> Result<String> {
        if encrypted.len() < NONCE_SIZE {
            return Err(EncryptionError::InvalidNonce.into());
        }

        let (nonce_bytes, ciphertext) = encrypted.split_at(NONCE_SIZE);
        let nonce = Nonce::from_slice(nonce_bytes);

        let dir_key = self.derive_file_key(dir_id)?;
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(dir_key.as_bytes()));

        let plaintext = cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| EncryptionError::DecryptionFailed)?;

        String::from_utf8(plaintext).map_err(|_| EncryptionError::DecryptionFailed.into())
    }

    /// Hash a filename for lookup (HMAC-like construction)
    pub fn hash_filename(&self, dir_id: &FileId, filename: &str) -> [u8; 32] {
        let dir_key = self.derive_file_key(dir_id).unwrap();

        let mut hasher = <Sha256 as Digest>::new();
        hasher.update(dir_key.as_bytes());
        hasher.update(filename.as_bytes());

        let result = hasher.finalize();
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&result);
        hash
    }

    /// Get the block size
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Encrypt a data block (optimized version using cached file key)
    pub fn encrypt_block(
        &self,
        file_id: &FileId,
        block_num: u64,
        plaintext: &[u8],
    ) -> Result<Vec<u8>> {
        // Get cached file key (fast operation)
        let file_key = self.derive_file_key(file_id)?;
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(file_key.as_bytes()));

        // Generate deterministic nonce using block number + random suffix
        // This ensures each block has a unique nonce while maintaining security
        let mut nonce_bytes = [0u8; NONCE_SIZE];
        nonce_bytes[..8].copy_from_slice(&block_num.to_le_bytes());
        rand::thread_rng().fill(&mut nonce_bytes[8..]);
        let nonce = Nonce::from_slice(&nonce_bytes);

        // Encrypt data
        let ciphertext = cipher
            .encrypt(nonce, plaintext)
            .map_err(|_| VsockFuseError::Encryption(EncryptionError::EncryptionFailed))?;

        // Combine nonce and ciphertext
        let mut result = nonce_bytes.to_vec();
        result.extend_from_slice(&ciphertext);

        Ok(result)
    }

    /// Decrypt a data block (optimized version using cached file key)
    pub fn decrypt_block(
        &self,
        file_id: &FileId,
        _block_num: u64,
        encrypted_data: &[u8],
    ) -> Result<Vec<u8>> {
        if encrypted_data.len() < NONCE_SIZE + TAG_SIZE {
            return Err(VsockFuseError::Encryption(EncryptionError::InvalidNonce));
        }

        let (nonce_bytes, ciphertext) = encrypted_data.split_at(NONCE_SIZE);
        let nonce = Nonce::from_slice(nonce_bytes);

        // Get cached file key (fast operation)
        let file_key = self.derive_file_key(file_id)?;
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(file_key.as_bytes()));

        // Decrypt the block
        cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| VsockFuseError::Encryption(EncryptionError::DecryptionFailed))
    }

    /// Read a range of data from an encrypted file
    pub async fn read_file_range(
        &self,
        file_id: &FileId,
        metadata: &crate::FileMetadata,
        storage: Arc<dyn crate::storage::BlobStorage>,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>> {
        // Calculate block range
        let start_block = offset / self.block_size as u64;
        let end_block = (offset + size as u64 - 1) / self.block_size as u64;
        let start_offset = (offset % self.block_size as u64) as usize;

        let mut result = Vec::with_capacity(size);

        for block_num in start_block..=end_block {
            // Find block location
            let block_loc = metadata
                .blocks
                .iter()
                .find(|b| b.block_num == block_num)
                .ok_or_else(|| VsockFuseError::InvalidArgument {
                    message: format!("Block {block_num} not found"),
                })?;

            // Get blob data
            let blob_data = storage.get_blob(block_loc.blob_id).await?;

            // Extract block data from blob
            let start = block_loc.offset as usize;
            let end = start + block_loc.encrypted_size;
            if end > blob_data.data.len() {
                return Err(VsockFuseError::Encryption(
                    EncryptionError::DecryptionFailed,
                ));
            }
            let encrypted_block = &blob_data.data[start..end];

            // Decrypt block
            let decrypted = self.decrypt_block(file_id, block_num, encrypted_block)?;

            // Calculate range within this block
            let block_start = if block_num == start_block {
                start_offset
            } else {
                0
            };
            let block_end = if block_num == end_block {
                std::cmp::min(
                    decrypted.len(),
                    ((offset + size as u64) - block_num * self.block_size as u64) as usize,
                )
            } else {
                decrypted.len()
            };

            result.extend_from_slice(&decrypted[block_start..block_end]);
        }

        Ok(result)
    }

    /// Write a range of data to an encrypted file
    pub async fn write_file_range(
        &self,
        file_id: &FileId,
        metadata: &mut crate::FileMetadata,
        storage: Arc<dyn crate::storage::BlobStorage>,
        offset: u64,
        data: &[u8],
    ) -> Result<usize> {
        use crate::{BlobId, BlockLocation, TierHint};

        // Calculate block range
        let start_block = offset / self.block_size as u64;
        let end_block = (offset + data.len() as u64 - 1) / self.block_size as u64;

        let mut written = 0;
        let mut data_offset = 0;

        for block_num in start_block..=end_block {
            let block_offset = if block_num == start_block {
                (offset % self.block_size as u64) as usize
            } else {
                0
            };

            // Calculate how much data to write to this block
            let block_write_size =
                std::cmp::min(self.block_size - block_offset, data.len() - data_offset);

            // Read existing block if partial write
            let mut block_data = if block_offset != 0 || block_write_size != self.block_size {
                // Try to read existing block
                if let Some(block_loc) = metadata.blocks.iter().find(|b| b.block_num == block_num) {
                    let blob_data = storage.get_blob(block_loc.blob_id).await?;
                    let start = block_loc.offset as usize;
                    let end = start + block_loc.encrypted_size;
                    let encrypted_block = &blob_data.data[start..end];
                    self.decrypt_block(file_id, block_num, encrypted_block)?
                } else {
                    vec![0; self.block_size]
                }
            } else {
                vec![0; self.block_size]
            };

            // Write data to block
            block_data[block_offset..block_offset + block_write_size]
                .copy_from_slice(&data[data_offset..data_offset + block_write_size]);

            // Encrypt block
            let encrypted_block = self.encrypt_block(file_id, block_num, &block_data)?;

            // Store block
            let blob_id = BlobId::new();
            storage
                .store_blob(blob_id, encrypted_block.clone(), TierHint::PreferHot)
                .await?;

            // Update or add block location
            let block_loc = BlockLocation {
                block_num,
                blob_id,
                offset: 0,
                encrypted_size: encrypted_block.len(),
            };

            if let Some(existing) = metadata
                .blocks
                .iter_mut()
                .find(|b| b.block_num == block_num)
            {
                *existing = block_loc;
            } else {
                metadata.blocks.push(block_loc);
                metadata.blocks.sort_by_key(|b| b.block_num);
            }

            written += block_write_size;
            data_offset += block_write_size;
        }

        // Update file size
        let new_size = offset + data.len() as u64;
        if new_size > metadata.size {
            metadata.size = new_size;
        }

        Ok(written)
    }
}

/// Key manager for handling master key lifecycle
pub struct KeyManager {
    /// Current master key
    current_key: Arc<MasterKey>,
    /// Key rotation interval
    _rotation_interval: std::time::Duration,
}

impl KeyManager {
    /// Create a new key manager
    pub fn new(master_key: MasterKey, rotation_interval: std::time::Duration) -> Self {
        Self {
            current_key: Arc::new(master_key),
            _rotation_interval: rotation_interval,
        }
    }

    /// Get the current master key
    pub fn current_key(&self) -> Arc<MasterKey> {
        Arc::clone(&self.current_key)
    }

    /// Derive master key from enclave attestation
    pub fn derive_from_attestation(
        attestation: &[u8],
        user_seed: Option<&[u8]>,
    ) -> Result<MasterKey> {
        let mut hasher = <Sha256 as Digest>::new();

        // Add attestation data
        hasher.update(attestation);

        // Add user seed if provided
        if let Some(seed) = user_seed {
            hasher.update(seed);
        }

        // Add random component
        let mut random = [0u8; 32];
        rand::thread_rng().fill(&mut random);
        hasher.update(random);

        let result = hasher.finalize();
        let mut key = [0u8; KEY_SIZE];
        key.copy_from_slice(&result);

        Ok(MasterKey(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encryption_roundtrip() {
        let master_key = MasterKey::generate();
        let layer = EncryptionLayer::new(master_key, 4096).unwrap();

        let file_id = FileId::new();
        let plaintext = b"Hello, encrypted world!";

        let encrypted = layer
            .encrypt_block_structured(&file_id, 0, plaintext)
            .unwrap();
        let decrypted = layer
            .decrypt_block_structured(&file_id, &encrypted)
            .unwrap();

        assert_eq!(plaintext, decrypted.as_slice());
    }

    #[test]
    fn test_different_blocks_different_nonces() {
        let master_key = MasterKey::generate();
        let layer = EncryptionLayer::new(master_key, 4096).unwrap();

        let file_id = FileId::new();
        let plaintext = b"Same data";

        let block1 = layer
            .encrypt_block_structured(&file_id, 0, plaintext)
            .unwrap();
        let block2 = layer
            .encrypt_block_structured(&file_id, 1, plaintext)
            .unwrap();

        assert_ne!(block1.nonce, block2.nonce);
        assert_ne!(block1.ciphertext, block2.ciphertext);
    }

    #[test]
    fn test_metadata_encryption() {
        let master_key = MasterKey::generate();
        let layer = EncryptionLayer::new(master_key, 4096).unwrap();

        let metadata = b"Secret metadata";

        let encrypted = layer.encrypt_metadata(metadata).unwrap();
        let decrypted = layer.decrypt_metadata(&encrypted).unwrap();

        assert_eq!(metadata, decrypted.as_slice());
    }

    #[test]
    fn test_filename_encryption() {
        let master_key = MasterKey::generate();
        let layer = EncryptionLayer::new(master_key, 4096).unwrap();

        let dir_id = FileId::new();
        let filename = "secret_file.txt";

        let encrypted = layer.encrypt_filename(&dir_id, filename).unwrap();
        let decrypted = layer.decrypt_filename(&dir_id, &encrypted).unwrap();

        assert_eq!(filename, decrypted);
    }

    #[test]
    fn test_filename_hash_deterministic() {
        let master_key = MasterKey::generate();
        let layer = EncryptionLayer::new(master_key, 4096).unwrap();

        let dir_id = FileId::new();
        let filename = "test.txt";

        let hash1 = layer.hash_filename(&dir_id, filename);
        let hash2 = layer.hash_filename(&dir_id, filename);

        assert_eq!(hash1, hash2);
    }
}
