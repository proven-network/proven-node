//! Implementation of key-value storage using files on disk, for local
//! development.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;

use error::Error;

use std::convert::Infallible;
use std::error::Error as StdError;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::marker::PhantomData;
use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use proven_store::{Store, Store1, Store2, Store3};
use tokio::fs;
use tokio::io::{self, AsyncWriteExt};

/// KV store using files on disk.
#[derive(Clone, Debug)]
pub struct FsStore<T = Bytes, DE = Infallible, SE = Infallible>
where
    Self: Clone + Debug + Send + Sync + 'static,
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    dir: PathBuf,
    _marker: PhantomData<T>,
    _marker2: PhantomData<DE>,
    _marker3: PhantomData<SE>,
}

impl<T, DE, SE> FsStore<T, DE, SE>
where
    Self: Clone + Debug + Send + Sync + 'static,
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    /// Creates a new `FsStore` with the specified directory.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            _marker: PhantomData,
            _marker2: PhantomData,
            _marker3: PhantomData,
        }
    }

    fn get_file_path(&self, key: &str) -> PathBuf {
        self.dir.join(key)
    }
}

#[async_trait]
impl<T, DE, SE> Store<T, DE, SE> for FsStore<T, DE, SE>
where
    Self: Clone + Debug + Send + Sync + 'static,
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = DE>
        + TryInto<Bytes, Error = SE>
        + 'static,
{
    type Error = Error<DE, SE>;

    async fn del<K: Into<String> + Send>(&self, key: K) -> Result<(), Self::Error> {
        let path = self.get_file_path(&key.into());
        fs::remove_file(path)
            .await
            .map_err(|e| Error::Io("error deleting file", e))?;
        Ok(())
    }

    async fn get<K: Into<String> + Send>(&self, key: K) -> Result<Option<T>, Self::Error> {
        let path = self.get_file_path(&key.into());
        match fs::read(path).await {
            Ok(data) => {
                let bytes: Bytes = data.into();
                let value: T = bytes.try_into().map_err(|e| Error::Deserialize(e))?;

                Ok(Some(value))
            }
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::Io("error reading file", e)),
        }
    }

    async fn keys(&self) -> Result<Vec<String>, Self::Error> {
        let mut entries = fs::read_dir(&self.dir)
            .await
            .map_err(|e| Error::Io("error reading directory", e))?;
        let mut keys = Vec::new();

        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| Error::Io("error reading directory entry", e))?
        {
            if let Some(key) = entry.file_name().to_str() {
                keys.push(key.to_string());
            }
        }

        Ok(keys)
    }

    async fn put<K: Into<String> + Send>(&self, key: K, value: T) -> Result<(), Self::Error> {
        let path = self.get_file_path(&key.into());
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)
                    .await
                    .map_err(|e| Error::Io("error creating directory", e))?;
            }
        }
        let mut file = fs::File::create(path)
            .await
            .map_err(|e| Error::Io("error creating file", e))?;
        let value: Bytes = value.try_into().map_err(|e| Error::Serialize(e))?;
        file.write_all(&value)
            .await
            .map_err(|e| Error::Io("error writing file", e))?;
        Ok(())
    }
}

macro_rules! impl_scoped_store {
    ($index:expr, $parent:ident, $parent_trait:ident, $doc:expr) => {
        preinterpret::preinterpret! {
            [!set! #name = [!ident! FsStore $index]]
            [!set! #trait_name = [!ident! Store $index]]

            #[doc = $doc]
            pub struct #name<T = Bytes, DE = Infallible, SE = Infallible>
            where
            Self: Debug + Send + Sync + 'static,
            DE: Send + StdError + Sync + 'static,
            SE: Send + StdError + Sync + 'static,
            T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = DE>
                    + TryInto<Bytes, Error = SE>
                    + 'static,
            {
                dir: PathBuf,
                _marker: PhantomData<T>,
                _marker2: PhantomData<DE>,
                _marker3: PhantomData<SE>,
            }

            impl<T, DE, SE> Clone for $parent<T, DE, SE>
            where
                Self: Debug + Send + Sync + 'static,
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = DE>
                    + TryInto<Bytes, Error = SE>
                    + 'static,
            {
                fn clone(&self) -> Self {
                    Self {
                        dir: self.dir.clone(),
                        _marker: PhantomData,
                        _marker2: PhantomData,
                        _marker3: PhantomData,
                    }
                }
            }

            impl<T, DE, SE> Debug for $parent<T, DE, SE>
            where
                Self: Clone + Send + Sync + 'static,
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = DE>
                    + TryInto<Bytes, Error = SE>
                    + 'static,
            {
                fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
                    f.debug_struct(stringify!(#name))
                        .field("dir", &self.dir)
                        .finish()
                }
            }

            impl<T, DE, SE> #name<T, DE, SE>
            where
                Self: Debug + Send + Sync + 'static,
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = DE>
                    + TryInto<Bytes, Error = SE>
                    + 'static,
            {
                /// Creates a new `#name` with the specified directory.
                #[must_use]
                pub fn new(dir: impl Into<PathBuf>) -> Self {
                    Self {
                        dir: dir.into(),
                        _marker: PhantomData,
                        _marker2: PhantomData,
                        _marker3: PhantomData,
                    }
                }
            }

            #[async_trait]
            impl<T, DE, SE> #trait_name<T, DE, SE> for #name<T, DE, SE>
            where
                Self: Clone + Debug + Send + Sync + 'static,
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = DE>
                    + TryInto<Bytes, Error = SE>
                    + 'static,
            {
                type Error = Error<DE, SE>;
                type Scoped = $parent<T, DE, SE>;

                fn [!ident! scope_ $index]<S: Into<String> + Send>(&self, scope: S) -> Self::Scoped {
                    let mut dir = self.dir.clone();
                    dir.push(scope.into());
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
        store.del(key.clone()).await.unwrap();
        let result = store.get(key).await.unwrap();

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_scope() {
        let dir = tempdir().unwrap();
        let store = FsStore1::new(dir.path().to_path_buf());

        let scoped_store = store.scope_1("scope");

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
        store.del(key).await.expect("Failed to delete value");
        let retrieved_value = store.get(key).await.expect("Failed to get value");
        assert_eq!(retrieved_value, None);
    }
}
