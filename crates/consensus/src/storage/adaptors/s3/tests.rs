//! Tests for S3 storage adaptor

#[cfg(test)]
mod s3_tests {
    use super::super::*;
    use crate::storage::types::{StorageKey, StorageNamespace};

    #[test]
    fn test_config_default() {
        let config = S3StorageConfig::default();
        assert_eq!(config.s3.region, "us-east-1");
        assert_eq!(config.s3.prefix, "consensus");
        assert!(config.s3.use_one_zone);
        assert_eq!(config.batch.max_size_bytes, 5 * 1024 * 1024);
        assert_eq!(config.wal.vsock_port, 5000);
    }

    #[test]
    fn test_key_mapper() {
        let mapper = keys::S3KeyMapper::new("test-prefix");

        // Test log key mapping
        let namespace = StorageNamespace::new("logs");
        let key = StorageKey::from_u64(12345);
        let s3_key = mapper.to_s3_key(&namespace, &key);
        assert_eq!(s3_key, "test-prefix/logs/logs/00000000000000012345");

        // Test regular key mapping
        let namespace = StorageNamespace::new("metadata");
        let key = StorageKey::new(b"test-key".to_vec());
        let s3_key = mapper.to_s3_key(&namespace, &key);
        assert!(s3_key.starts_with("test-prefix/metadata/"));
    }

    // Integration tests would go here, using LocalStack
    // These would be marked with #[ignore] and run separately
}
