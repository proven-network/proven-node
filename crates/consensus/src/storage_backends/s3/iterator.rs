//! S3 iterator implementation

use crate::storage_backends::types::{
    StorageError, StorageIterator, StorageKey, StorageNamespace, StorageResult, StorageValue,
};
use aws_sdk_s3::Client as S3Client;
use std::{collections::VecDeque, ops::RangeBounds, sync::Arc};
use tracing::{debug, error};

use super::{encryption::EncryptionManager, keys::S3KeyMapper};

/// Iterator over S3 objects
pub struct S3Iterator {
    client: S3Client,
    bucket: String,
    prefix: String,
    start_after: Option<String>,
    end_before: Option<String>,
    encryptor: Arc<EncryptionManager>,
    key_mapper: Arc<S3KeyMapper>,
    namespace: StorageNamespace,

    // Buffered results
    buffer: VecDeque<(StorageKey, StorageValue)>,
    continuation_token: Option<String>,
    is_exhausted: bool,

    // Stats
    total_fetched: usize,
    total_returned: usize,
}

impl S3Iterator {
    /// Create a new S3 iterator
    pub async fn new(
        client: S3Client,
        bucket: String,
        prefix: String,
        encryptor: Arc<EncryptionManager>,
        key_mapper: Arc<S3KeyMapper>,
        namespace: StorageNamespace,
    ) -> StorageResult<Self> {
        let mut iter = Self {
            client,
            bucket,
            prefix,
            start_after: None,
            end_before: None,
            encryptor,
            key_mapper,
            namespace,
            buffer: VecDeque::new(),
            continuation_token: None,
            is_exhausted: false,
            total_fetched: 0,
            total_returned: 0,
        };

        // Pre-fetch first batch
        iter.fetch_next_batch().await?;

        Ok(iter)
    }

    /// Create a new S3 iterator with range bounds
    pub async fn new_with_range<R>(
        client: S3Client,
        bucket: String,
        prefix: String,
        range: R,
        encryptor: Arc<EncryptionManager>,
        key_mapper: Arc<S3KeyMapper>,
        namespace: StorageNamespace,
    ) -> StorageResult<Self>
    where
        R: RangeBounds<StorageKey>,
    {
        let (_prefix, start_after, end_before) = key_mapper.range_to_s3_bounds(&namespace, range);

        let mut iter = Self {
            client,
            bucket,
            prefix,
            start_after,
            end_before,
            encryptor,
            key_mapper,
            namespace,
            buffer: VecDeque::new(),
            continuation_token: None,
            is_exhausted: false,
            total_fetched: 0,
            total_returned: 0,
        };

        // Pre-fetch first batch
        iter.fetch_next_batch().await?;

        Ok(iter)
    }

    /// Fetch the next batch of results from S3
    async fn fetch_next_batch(&mut self) -> StorageResult<()> {
        if self.is_exhausted {
            return Ok(());
        }

        debug!("Fetching S3 batch with prefix: {}", self.prefix);

        // Build list request
        let mut request = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&self.prefix)
            .max_keys(1000); // S3 max is 1000

        if let Some(token) = &self.continuation_token {
            request = request.continuation_token(token);
        } else if let Some(start) = &self.start_after {
            request = request.start_after(start);
        }

        // Execute request
        let response = request.send().await.map_err(|e| {
            StorageError::Io(std::io::Error::other(format!(
                "Failed to list S3 objects: {e}"
            )))
        })?;

        // Process results
        if let Some(contents) = response.contents {
            let mut batch_count = 0;

            for object in contents {
                if let Some(key) = object.key {
                    // Check end boundary
                    if let Some(end) = &self.end_before
                        && key >= *end
                    {
                        self.is_exhausted = true;
                        break;
                    }

                    // Fetch object data
                    match self.fetch_object(&key).await {
                        Ok(Some((storage_key, storage_value))) => {
                            self.buffer.push_back((storage_key, storage_value));
                            batch_count += 1;
                        }
                        Ok(None) => {
                            // Key mapping failed, skip
                            continue;
                        }
                        Err(e) => {
                            error!("Failed to fetch object {}: {}", key, e);
                            // Continue with other objects
                        }
                    }
                }
            }

            self.total_fetched += batch_count;
            debug!("Fetched {} objects in batch", batch_count);
        }

        // Update continuation token
        if response.is_truncated == Some(true) {
            self.continuation_token = response.next_continuation_token;
        } else {
            self.is_exhausted = true;
        }

        Ok(())
    }

    /// Fetch a single object from S3
    async fn fetch_object(
        &self,
        s3_key: &str,
    ) -> StorageResult<Option<(StorageKey, StorageValue)>> {
        // Parse key
        let (_namespace, storage_key) = match self.key_mapper.parse_s3_key(s3_key) {
            Some((ns, key)) if ns == self.namespace => (ns, key),
            _ => return Ok(None),
        };

        // Get object
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(s3_key)
            .send()
            .await
            .map_err(|e| {
                StorageError::Io(std::io::Error::other(format!(
                    "Failed to get S3 object: {e}"
                )))
            })?;

        // Collect body
        let data = response
            .body
            .collect()
            .await
            .map_err(|e| {
                StorageError::Io(std::io::Error::other(format!(
                    "Failed to read S3 body: {e}"
                )))
            })?
            .into_bytes();

        // Decrypt
        let decrypted = self.encryptor.decrypt_from_s3(&data).await?;

        Ok(Some((storage_key, StorageValue::new(decrypted))))
    }
}

impl StorageIterator for S3Iterator {
    fn next(&mut self) -> StorageResult<Option<(StorageKey, StorageValue)>> {
        // Return from buffer if available
        if let Some(item) = self.buffer.pop_front() {
            self.total_returned += 1;
            return Ok(Some(item));
        }

        // If buffer is empty and we're not exhausted, fetch more
        if !self.is_exhausted {
            // Block on async fetch (not ideal, but matches trait interface)
            let rt = tokio::runtime::Handle::current();
            rt.block_on(self.fetch_next_batch())?;

            // Try again from buffer
            if let Some(item) = self.buffer.pop_front() {
                self.total_returned += 1;
                return Ok(Some(item));
            }
        }

        // No more items
        debug!(
            "S3 iterator exhausted. Total fetched: {}, returned: {}",
            self.total_fetched, self.total_returned
        );
        Ok(None)
    }

    fn seek(&mut self, key: &StorageKey) -> StorageResult<()> {
        // Clear buffer
        self.buffer.clear();

        // Update start position
        self.start_after = Some(self.key_mapper.to_s3_key(&self.namespace, key));
        self.continuation_token = None;
        self.is_exhausted = false;

        // Fetch new batch
        let rt = tokio::runtime::Handle::current();
        rt.block_on(self.fetch_next_batch())?;

        Ok(())
    }

    fn valid(&self) -> bool {
        !self.buffer.is_empty() || !self.is_exhausted
    }
}
