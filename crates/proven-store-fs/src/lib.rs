mod error;

use error::Error;

use async_trait::async_trait;
use proven_store::Store;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::{self, AsyncWriteExt};

#[derive(Clone, Debug)]
pub struct FsStore {
    dir: PathBuf,
}

impl FsStore {
    pub fn new(dir: PathBuf) -> Self {
        FsStore { dir }
    }

    fn get_file_path(&self, key: &str) -> PathBuf {
        self.dir.join(key)
    }
}

#[async_trait]
impl Store for FsStore {
    type SE = Error;

    async fn del(&self, key: String) -> Result<(), Self::SE> {
        let path = self.get_file_path(&key);
        fs::remove_file(path).await?;
        Ok(())
    }

    async fn get(&self, key: String) -> Result<Option<Vec<u8>>, Self::SE> {
        let path = self.get_file_path(&key);
        match fs::read(path).await {
            Ok(data) => Ok(Some(data)),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::Io(e)),
        }
    }

    async fn put(&self, key: String, bytes: Vec<u8>) -> Result<(), Self::SE> {
        let path = self.get_file_path(&key);
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
