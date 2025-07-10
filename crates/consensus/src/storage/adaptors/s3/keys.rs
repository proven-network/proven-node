//! S3 key mapping and namespace management

use crate::storage::types::{StorageKey, StorageNamespace};
use std::ops::RangeBounds;

/// S3 key mapper for converting between storage keys and S3 object keys
#[derive(Clone, Debug)]
pub struct S3KeyMapper {
    prefix: String,
}

impl S3KeyMapper {
    /// Create a new key mapper
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.trim_end_matches('/').to_string(),
        }
    }

    /// Convert a storage namespace and key to S3 object key
    pub fn to_s3_key(&self, namespace: &StorageNamespace, key: &StorageKey) -> String {
        let ns = namespace.as_str();

        // Special handling for log entries (sequential access optimization)
        if ns == "logs" || ns.ends_with("/logs") {
            // Assume key is a u64 index for logs
            if key.as_bytes().len() == 8 {
                let index = u64::from_be_bytes(key.as_bytes().try_into().unwrap_or_default());
                return format!("{}/{}/logs/{:020}", self.prefix, ns, index);
            }
        }

        // Special handling for snapshots
        if ns == "snapshots" || ns.ends_with("/snapshots") {
            let key_str = String::from_utf8_lossy(key.as_bytes());
            return format!("{}/{}/snapshots/{}", self.prefix, ns, key_str);
        }

        // Special handling for stream data
        if ns.contains("/streams/") {
            let key_str = String::from_utf8_lossy(key.as_bytes());
            return format!("{}/{}/{}", self.prefix, ns, key_str);
        }

        // Default: hex-encoded key for safety
        let key_hex = hex::encode(key.as_bytes());
        format!("{}/{}/{}", self.prefix, ns, key_hex)
    }

    /// Convert S3 object key back to storage key
    pub fn parse_s3_key(&self, s3_key: &str) -> Option<(StorageNamespace, StorageKey)> {
        // Remove prefix
        let without_prefix = s3_key.strip_prefix(&self.prefix)?.trim_start_matches('/');

        // Split into namespace and key parts
        let parts: Vec<&str> = without_prefix.splitn(2, '/').collect();
        if parts.len() != 2 {
            return None;
        }

        let namespace = StorageNamespace::new(parts[0]);

        // Handle log entries
        if parts[1].starts_with("logs/") {
            if let Some(index_str) = parts[1].strip_prefix("logs/") {
                if let Ok(index) = index_str.parse::<u64>() {
                    return Some((namespace, StorageKey::from_u64(index)));
                }
            }
        }

        // Handle snapshots
        if parts[1].starts_with("snapshots/") {
            if let Some(id) = parts[1].strip_prefix("snapshots/") {
                return Some((namespace, StorageKey::new(id.as_bytes().to_vec())));
            }
        }

        // Default: try to decode as hex
        if let Ok(decoded) = hex::decode(parts[1]) {
            return Some((namespace, StorageKey::new(decoded)));
        }

        // Fallback: use as-is
        Some((namespace, StorageKey::new(parts[1].as_bytes().to_vec())))
    }

    /// Get the S3 prefix for a namespace
    pub fn namespace_prefix(&self, namespace: &StorageNamespace) -> String {
        format!("{}/{}", self.prefix, namespace.as_str())
    }

    /// Extract namespace from S3 prefix
    pub fn extract_namespace(&self, s3_prefix: &str) -> Option<String> {
        let without_prefix = s3_prefix
            .strip_prefix(&self.prefix)?
            .trim_start_matches('/');
        without_prefix.split('/').next().map(|s| s.to_string())
    }

    /// Convert a key range to S3 prefix and bounds
    pub fn range_to_s3_bounds<R>(
        &self,
        namespace: &StorageNamespace,
        range: R,
    ) -> (String, Option<String>, Option<String>)
    where
        R: RangeBounds<StorageKey>,
    {
        use std::ops::Bound;

        let prefix = self.namespace_prefix(namespace);

        let start_key = match range.start_bound() {
            Bound::Included(key) => Some(self.to_s3_key(namespace, key)),
            Bound::Excluded(key) => {
                // For excluded start, we need the next key
                let s3_key = self.to_s3_key(namespace, key);
                Some(self.next_s3_key(&s3_key))
            }
            Bound::Unbounded => None,
        };

        let end_key = match range.end_bound() {
            Bound::Included(key) => {
                // For included end, we need the next key
                let s3_key = self.to_s3_key(namespace, key);
                Some(self.next_s3_key(&s3_key))
            }
            Bound::Excluded(key) => Some(self.to_s3_key(namespace, key)),
            Bound::Unbounded => None,
        };

        (prefix, start_key, end_key)
    }

    /// Get the next S3 key (for range queries)
    fn next_s3_key(&self, key: &str) -> String {
        // Simple implementation: append a null byte
        format!("{}\0", key)
    }

    /// Get the S3 key for a snapshot
    pub fn snapshot_key(&self, namespace: &StorageNamespace, snapshot_id: &str) -> String {
        format!(
            "{}/{}/__snapshots__/{}",
            self.prefix,
            namespace.as_str(),
            snapshot_id
        )
    }

    /// Get the S3 prefix for snapshots in a namespace
    pub fn snapshot_prefix(&self, namespace: &StorageNamespace) -> String {
        format!("{}/{}/__snapshots__/", self.prefix, namespace.as_str())
    }

    /// Extract snapshot ID from S3 key
    pub fn extract_snapshot_id(&self, s3_key: &str) -> Option<String> {
        // Look for __snapshots__ in the path
        let parts: Vec<&str> = s3_key.split("/__snapshots__/").collect();
        if parts.len() == 2 {
            Some(parts[1].to_string())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_key_mapping() {
        let mapper = S3KeyMapper::new("consensus");
        let namespace = StorageNamespace::new("logs");
        let key = StorageKey::from_u64(12345);

        let s3_key = mapper.to_s3_key(&namespace, &key);
        assert_eq!(s3_key, "consensus/logs/logs/00000000000000012345");

        let (ns, k) = mapper.parse_s3_key(&s3_key).unwrap();
        assert_eq!(ns, namespace);
        assert_eq!(k, key);
    }

    #[test]
    fn test_regular_key_mapping() {
        let mapper = S3KeyMapper::new("consensus");
        let namespace = StorageNamespace::new("metadata");
        let key = StorageKey::new(b"test-key".to_vec());

        let s3_key = mapper.to_s3_key(&namespace, &key);
        assert_eq!(s3_key, "consensus/metadata/746573742d6b6579");

        let (ns, k) = mapper.parse_s3_key(&s3_key).unwrap();
        assert_eq!(ns, namespace);
        assert_eq!(k, key);
    }
}
