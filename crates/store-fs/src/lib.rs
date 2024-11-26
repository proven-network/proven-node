mod error;

use error::Error;

use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use proven_store::{Store, Store1, Store2, Store3};
use tokio::fs;
use tokio::io::{self, AsyncWriteExt};

#[derive(Clone, Debug)]
pub struct FsStore {
    dir: PathBuf,
}

/// FsStore is a file system implementation of the `Store`, `Store2`, and `Store3` traits.
/// It uses the file system to store key-value pairs, where keys are strings and values are byte vectors.
/// The store supports optional scoping of keys using a directory.
impl FsStore {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        FsStore { dir: dir.into() }
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
        fs::remove_file(path).await?;
        Ok(())
    }

    async fn get<K: Into<String> + Send>(&self, key: K) -> Result<Option<Bytes>, Self::Error> {
        let path = self.get_file_path(&key.into());
        match fs::read(path).await {
            Ok(data) => Ok(Some(Bytes::from(data))),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::Io(e)),
        }
    }

    async fn keys(&self) -> Result<Vec<String>, Self::Error> {
        let mut entries = fs::read_dir(&self.dir).await?;
        let mut keys = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
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
                fs::create_dir_all(parent).await?;
            }
        }
        let mut file = fs::File::create(path).await?;
        file.write_all(&bytes).await?;
        Ok(())
    }
}

#[async_trait]
impl Store1 for FsStore {
    type Error = Error;
    type Scoped = Self;

    fn scope<S: Into<String> + Send>(&self, scope: S) -> Self::Scoped {
        let mut dir = self.dir.clone();
        dir.push(scope.into());
        Self::new(dir)
    }
}

#[async_trait]
impl Store2 for FsStore {
    type Error = Error;
    type Scoped = Self;

    fn scope<S: Into<String> + Send>(&self, scope: S) -> Self::Scoped {
        let mut dir = self.dir.clone();
        dir.push(scope.into());
        Self::new(dir)
    }
}

#[async_trait]
impl Store3 for FsStore {
    type Error = Error;
    type Scoped = Self;

    fn scope<S: Into<String> + Send>(&self, scope: S) -> Self::Scoped {
        let mut dir = self.dir.clone();
        dir.push(scope.into());
        Self::new(dir)
    }
}

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
