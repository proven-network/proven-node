//! Implementation of key-value storage using files on disk, for local
//! development.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::too_many_lines)]

mod error;

use error::Error;

use std::convert::Infallible;
use std::error::Error as StdError;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::fs;
use std::io::Write;
use std::marker::PhantomData;
use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use proven_store::{Store, Store1, Store2, Store3};

/// KV store using files on disk.
#[derive(Debug)]
pub struct FsStore<T = Bytes, D = Infallible, S = Infallible>
where
    T: Clone + Debug + Send + Sync + 'static,
    D: Send + StdError + Sync + 'static,
    S: Send + StdError + Sync + 'static,
{
    dir: PathBuf,
    _marker: PhantomData<(T, D, S)>,
}

impl<T, D, S> Clone for FsStore<T, D, S>
where
    D: Send + StdError + Sync + 'static,
    S: Send + StdError + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
{
    fn clone(&self) -> Self {
        Self {
            dir: self.dir.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T, D, S> FsStore<T, D, S>
where
    Self: Debug + Send + Sync + 'static,
    D: Send + StdError + Sync + 'static,
    S: Send + StdError + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
{
    /// Creates a new `FsStore` with the specified directory.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            _marker: PhantomData,
        }
    }

    fn get_file_path<K: AsRef<str>>(&self, key: K) -> PathBuf {
        self.dir.join(key.as_ref())
    }

    fn list_files(&self, prefix: Option<&str>) -> Result<Vec<String>, Error> {
        fn visit_dirs(
            dir: &std::path::Path,
            base: &std::path::Path,
            prefix: Option<&str>,
            out: &mut Vec<String>,
        ) -> std::io::Result<()> {
            if dir.is_dir() {
                for entry in fs::read_dir(dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    if path.is_dir() {
                        visit_dirs(&path, base, prefix, out)?;
                    } else if let Ok(rel_path) = path.strip_prefix(base)
                        && let Some(path_str) = rel_path.to_str()
                    {
                        if let Some(prefix) = prefix {
                            if path_str.starts_with(prefix) {
                                out.push(path_str.to_string());
                            }
                        } else {
                            out.push(path_str.to_string());
                        }
                    }
                }
            }
            Ok(())
        }

        let mut keys = Vec::new();
        visit_dirs(&self.dir, &self.dir, prefix, &mut keys)
            .map_err(|e| Error::Io("error walking directory", e))?;
        Ok(keys)
    }
}

#[async_trait]
impl<T, D, S> Store<T, D, S> for FsStore<T, D, S>
where
    Self: Clone + Debug + Send + Sync + 'static,
    D: Send + StdError + Sync + 'static,
    S: Send + StdError + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
{
    type Error = Error;

    async fn delete<K>(&self, key: K) -> Result<(), Self::Error>
    where
        K: AsRef<str> + Send,
    {
        let path = self.get_file_path(key);
        fs::remove_file(path).map_err(|e| Error::Io("error deleting file", e))?;
        Ok(())
    }

    async fn get<K: AsRef<str> + Send>(&self, key: K) -> Result<Option<T>, Self::Error> {
        let path = self.get_file_path(key);
        match fs::read(path) {
            Ok(data) => {
                let bytes: Bytes = data.into();
                let value: T = bytes
                    .try_into()
                    .map_err(|e: D| Error::Deserialize(e.to_string()))?;
                Ok(Some(value))
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::Io("error reading file", e)),
        }
    }

    async fn keys(&self) -> Result<Vec<String>, Self::Error> {
        self.list_files(None)
    }

    async fn keys_with_prefix<P>(&self, prefix: P) -> Result<Vec<String>, Self::Error>
    where
        P: AsRef<str> + Send,
    {
        self.list_files(Some(prefix.as_ref()))
    }

    async fn put<K: AsRef<str> + Send>(&self, key: K, value: T) -> Result<(), Self::Error> {
        let path = self.get_file_path(key);
        if let Some(parent) = path.parent()
            && !parent.exists()
        {
            fs::create_dir_all(parent).map_err(|e| Error::Io("error creating directory", e))?;
        }
        let value: Bytes = value
            .try_into()
            .map_err(|e| Error::Serialize(e.to_string()))?;
        let mut file = fs::File::create(path).map_err(|e| Error::Io("error creating file", e))?;
        file.write_all(&value)
            .map_err(|e| Error::Io("error writing file", e))?;
        Ok(())
    }
}

macro_rules! impl_scoped_store {
    ($index:expr_2021, $parent:ident, $parent_trait:ident, $doc:expr_2021) => {
        paste::paste! {
            #[doc = $doc]
            pub struct [< FsStore $index >]<T = Bytes, D = Infallible, S = Infallible>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Send + StdError + Sync + 'static,
                S: Send + StdError + Sync + 'static,
            {
                dir: PathBuf,
                _marker: PhantomData<(T, D, S)>,
            }

            impl<T, D, S> Clone for [< FsStore $index >]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Send + StdError + Sync + 'static,
                S: Send + StdError + Sync + 'static,
            {
                fn clone(&self) -> Self {
                    Self {
                        dir: self.dir.clone(),
                        _marker: PhantomData,
                    }
                }
            }

            impl<T, D, S> Debug for [< FsStore $index >]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Send + StdError + Sync + 'static,
                S: Send + StdError + Sync + 'static,
            {
                fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
                    f.debug_struct(stringify!([< FsStore $index >]))
                        .field("dir", &self.dir)
                        .finish()
                }
            }

            impl<T, D, S> [< FsStore $index >]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Send + StdError + Sync + 'static,
                S: Send + StdError + Sync + 'static,
            {
                /// Creates a new `[< FsStore $index >]` with the specified directory.
                #[must_use]
                pub fn new(dir: impl Into<PathBuf>) -> Self {
                    Self {
                        dir: dir.into(),
                        _marker: PhantomData,
                    }
                }
            }

            #[async_trait]
            impl<T, D, S> [< Store $index >]<T, D, S> for [< FsStore $index >]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Send + StdError + Sync + 'static,
                S: Send + StdError + Sync + 'static,
            {
                type Error = Error;
                type Scoped = $parent<T, D, S>;

                fn scope<K>(&self, scope: K) -> Self::Scoped
                where
                    K: AsRef<str> + Send,
                {
                    let mut dir = self.dir.clone();
                    dir.push(scope.as_ref());
                    Self::Scoped::new(dir)
                }
            }
        }
    };
}

impl_scoped_store!(
    1,
    FsStore,
    Store,
    "A single-scoped KV store using the filesystem."
);
impl_scoped_store!(
    2,
    FsStore1,
    Store1,
    "A double-scoped KV store using the filesystem."
);
impl_scoped_store!(
    3,
    FsStore2,
    Store2,
    "A triple-scoped KV store using the filesystem."
);

#[cfg(test)]
mod tests {
    use super::*;

    use std::fmt::Display;

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
        let store: FsStore<Bytes> = FsStore::new(dir.path().to_path_buf());

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
        store.delete(key.clone()).await.unwrap();
        let result = store.get(key).await.unwrap();

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_scope() {
        let dir = tempdir().unwrap();
        let store = FsStore1::new(dir.path().to_path_buf());

        let scoped_store = store.scope("scope");

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

    #[derive(Clone, Debug, PartialEq)]
    struct MyType {
        data: String,
    }

    #[derive(Debug, Clone)]
    struct MyError;

    impl Display for MyError {
        fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
            write!(f, "MyError")
        }
    }

    impl StdError for MyError {}

    impl TryFrom<Bytes> for MyType {
        type Error = MyError;

        fn try_from(value: Bytes) -> Result<Self, Self::Error> {
            let data = String::from_utf8(value.to_vec()).map_err(|_| MyError)?;
            Ok(Self { data })
        }
    }

    impl TryInto<Bytes> for MyType {
        type Error = MyError;

        fn try_into(self) -> Result<Bytes, Self::Error> {
            Ok(Bytes::from(self.data))
        }
    }

    #[tokio::test]
    async fn test_non_bytes() {
        let store = FsStore::<MyType, MyError, MyError>::new("/tmp/test_store");

        // Test put
        let key = "test_key";
        let value = MyType {
            data: "test_value".to_string(),
        };
        store
            .put(key, value.clone())
            .await
            .expect("Failed to put value");

        // Test get
        let retrieved_value = store.get(key).await.expect("Failed to get value");
        assert_eq!(retrieved_value, Some(value));

        // Test del
        store.delete(key).await.expect("Failed to delete value");
        let retrieved_value = store.get(key).await.expect("Failed to get value");
        assert_eq!(retrieved_value, None);
    }

    #[tokio::test]
    async fn test_non_bytes_scoped() {
        let store = FsStore1::<MyType, MyError, MyError>::new("/tmp/test_store").scope("scope");

        // Test put
        let key = "test_key";
        let value = MyType {
            data: "test_value".to_string(),
        };
        store
            .put(key, value.clone())
            .await
            .expect("Failed to put value");

        // Test get
        let retrieved_value = store.get(key).await.expect("Failed to get value");
        assert_eq!(retrieved_value, Some(value));

        // Test del
        store.delete(key).await.expect("Failed to delete value");
        let retrieved_value = store.get(key).await.expect("Failed to get value");
        assert_eq!(retrieved_value, None);
    }

    #[tokio::test]
    async fn test_keys_with_prefix() {
        let dir = tempdir().unwrap();
        let store = FsStore::new(dir.path());

        store.put("test/one.txt", Bytes::from("one")).await.unwrap();
        store.put("test/two.txt", Bytes::from("two")).await.unwrap();
        store
            .put("other/three.txt", Bytes::from("three"))
            .await
            .unwrap();

        let keys = store.keys_with_prefix("test/".to_string()).await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"test/one.txt".to_string()));
        assert!(keys.contains(&"test/two.txt".to_string()));
    }

    #[tokio::test]
    async fn test_nested_directories() {
        let dir = tempdir().unwrap();
        let store = FsStore::new(dir.path());

        store
            .put("a/b/c/file.txt", Bytes::from("content"))
            .await
            .unwrap();
        store
            .put("a/b/file2.txt", Bytes::from("content2"))
            .await
            .unwrap();

        let keys = store.keys_with_prefix("a/b/".to_string()).await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"a/b/c/file.txt".to_string()));
        assert!(keys.contains(&"a/b/file2.txt".to_string()));

        let keys = store.keys_with_prefix("a/b/c/".to_string()).await.unwrap();
        assert_eq!(keys.len(), 1);
        assert!(keys.contains(&"a/b/c/file.txt".to_string()));
    }
}
