//! Implementation of key-value storage using files on disk, for local
//! development.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;

use error::Error;

use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use proven_store::{Store, Store1, Store2, Store3};
use tokio::fs;
use tokio::io::{self, AsyncWriteExt};

/// KV store using files on disk.
#[derive(Clone, Debug)]
pub struct FsStore {
    dir: PathBuf,
}

impl FsStore {
    /// Creates a new `FsStore` with the specified directory.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    fn get_file_path(&self, key: &str) -> PathBuf {
        self.dir.join(key)
    }
}

#[async_trait]
impl Store for FsStore {
    type Error = Error;

    async fn del<K: Into<String> + Send>(&self, key: K) -> Result<(), Self::Error> {
        let path = self.get_file_path(&key.into());
        fs::remove_file(path)
            .await
            .map_err(|e| Error::IoError("error deleting file", e))?;
        Ok(())
    }

    async fn get<K: Into<String> + Send>(&self, key: K) -> Result<Option<Bytes>, Self::Error> {
        let path = self.get_file_path(&key.into());
        match fs::read(path).await {
            Ok(data) => Ok(Some(Bytes::from(data))),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::IoError("error reading file", e)),
        }
    }

    async fn keys(&self) -> Result<Vec<String>, Self::Error> {
        let mut entries = fs::read_dir(&self.dir)
            .await
            .map_err(|e| Error::IoError("error reading directory", e))?;
        let mut keys = Vec::new();

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| Error::IoError("error reading directory entry", e))?
        {
            if let Some(key) = entry.file_name().to_str() {
                keys.push(key.to_string());
            }
        }

        Ok(keys)
    }

    async fn put<K: Into<String> + Send>(&self, key: K, bytes: Bytes) -> Result<(), Self::Error> {
        let path = self.get_file_path(&key.into());
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)
                    .await
                    .map_err(|e| Error::IoError("error creating directory", e))?;
            }
        }
        let mut file = fs::File::create(path)
            .await
            .map_err(|e| Error::IoError("error creating file", e))?;
        file.write_all(&bytes)
            .await
            .map_err(|e| Error::IoError("error writing file", e))?;
        Ok(())
    }
}

macro_rules! impl_scoped_store {
    ($name:ident, $parent:ident) => {
        #[async_trait]
        impl $name for FsStore {
            type Error = Error;
            type Scoped = Self;

            fn scope<S: Into<String> + Send>(&self, scope: S) -> Self::Scoped {
                let mut dir = self.dir.clone();
                dir.push(scope.into());
                Self::new(dir)
            }
        }
    };
}

impl_scoped_store!(Store1, Store);
impl_scoped_store!(Store2, Store1);
impl_scoped_store!(Store3, Store2);

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    #[tokio::test]
    async fn test_put_and_get() {
        let dir = tempdir().unwrap();
        let store = FsStore::new(dir.path().to_path_buf());

        let key = "test_key".to_string();
        let value = Bytes::from_static(b"test_value");

        store.put(key.clone(), value.clone()).await.unwrap();
        let result = store.get(key.clone()).await.unwrap();

        assert_eq!(result, Some(value));
    }

    #[tokio::test]
    async fn test_get_nonexistent_key() {
        let dir = tempdir().unwrap();
        let store = FsStore::new(dir.path().to_path_buf());

        let key = "nonexistent_key".to_string();
        let result = store.get(key).await.unwrap();

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_del() {
        let dir = tempdir().unwrap();
        let store = FsStore::new(dir.path().to_path_buf());

        let key = "test_key".to_string();
        let value = Bytes::from_static(b"test_value");

        store.put(key.clone(), value.clone()).await.unwrap();
        store.del(key.clone()).await.unwrap();
        let result = store.get(key).await.unwrap();

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_scope() {
        let dir = tempdir().unwrap();
        let store = FsStore::new(dir.path().to_path_buf());

        let scoped_store = Store2::scope(&store, "scope".to_string());

        let key = "test_key".to_string();
        let value = Bytes::from_static(b"test_value");

        scoped_store.put(key.clone(), value.clone()).await.unwrap();
        let result = scoped_store.get(key.clone()).await.unwrap();

        assert_eq!(result, Some(value));
    }

    #[tokio::test]
    async fn test_put_creates_directories() {
        let dir = tempdir().unwrap();
        let store = FsStore::new(dir.path().to_path_buf());

        let key = "nested/directory/test_key".to_string();
        let value = Bytes::from_static(b"test_value");

        store.put(key.clone(), value.clone()).await.unwrap();
        let result = store.get(key.clone()).await.unwrap();

        assert_eq!(result, Some(value));
    }
}
