use super::metadata::FsMetadata;
use super::StorageEntry;

use std::borrow::Cow;
use std::path::PathBuf;
use std::rc::Rc;

use bytes::Bytes;
use deno_core::{BufMutView, BufView, ResourceHandleFd};
use deno_io::fs::{File as DenoFile, FsResult, FsStat};
use proven_store::Store;
use serde::{Deserialize, Serialize};

/// File type used for storage (without store reference)
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct StorageFile {
    pub content: Bytes,
    pub metadata: FsMetadata,
}

/// Runtime file type with store reference
#[derive(Clone, Debug)]
pub struct File<S>
where
    S: Store<StorageEntry, serde_json::Error, serde_json::Error>,
{
    pub content: Bytes,
    pub path: PathBuf,
    pub metadata: FsMetadata,
    pub store: Option<S>,
}

impl<S> From<StorageFile> for File<S>
where
    S: Store<StorageEntry, serde_json::Error, serde_json::Error>,
{
    fn from(storage: StorageFile) -> Self {
        Self {
            content: storage.content,
            path: PathBuf::new(), // Path needs to be set after conversion
            metadata: storage.metadata,
            store: None,
        }
    }
}

impl<S> From<File<S>> for StorageFile
where
    S: Store<StorageEntry, serde_json::Error, serde_json::Error>,
{
    fn from(file: File<S>) -> Self {
        Self {
            content: file.content,
            metadata: file.metadata,
        }
    }
}

impl<S> File<S>
where
    S: Store<StorageEntry, serde_json::Error, serde_json::Error>,
{
    pub const fn new(content: Bytes, path: PathBuf, metadata: FsMetadata, store: S) -> Self {
        Self {
            content,
            path,
            metadata,
            store: Some(store),
        }
    }

    async fn persist(&self) -> FsResult<()> {
        let storage_entry = StorageEntry::File(self.clone().into());

        self.store
            .as_ref()
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "Store is None"))?
            .put(self.path.to_string_lossy().to_string(), storage_entry)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl<S> DenoFile for File<S>
where
    S: Store<StorageEntry, serde_json::Error, serde_json::Error> + 'static,
{
    fn read_sync(self: Rc<Self>, _buf: &mut [u8]) -> FsResult<usize> {
        todo!()
    }

    async fn read_byob(self: Rc<Self>, _buf: BufMutView) -> FsResult<(usize, BufMutView)> {
        todo!()
    }

    fn write_sync(self: Rc<Self>, _buf: &[u8]) -> FsResult<usize> {
        todo!()
    }

    async fn write(self: Rc<Self>, _buf: BufView) -> FsResult<deno_core::WriteOutcome> {
        todo!()
    }

    fn write_all_sync(self: Rc<Self>, buf: &[u8]) -> FsResult<()> {
        let this = self.as_ref();
        let mut cloned = this.clone();
        cloned.content = Bytes::copy_from_slice(buf);

        // Use block_on to run the async persist in sync context
        futures::executor::block_on(cloned.persist())?;
        Ok(())
    }

    async fn write_all(self: Rc<Self>, _buf: BufView) -> FsResult<()> {
        todo!()
    }

    fn read_all_sync(self: Rc<Self>) -> FsResult<Cow<'static, [u8]>> {
        Ok(Cow::Owned(self.content.to_vec()))
    }

    async fn read_all_async(self: Rc<Self>) -> FsResult<Cow<'static, [u8]>> {
        todo!()
    }

    fn chmod_sync(self: Rc<Self>, _pathmode: u32) -> FsResult<()> {
        todo!()
    }

    async fn chmod_async(self: Rc<Self>, _mode: u32) -> FsResult<()> {
        todo!()
    }

    fn seek_sync(self: Rc<Self>, _pos: std::io::SeekFrom) -> FsResult<u64> {
        todo!()
    }

    async fn seek_async(self: Rc<Self>, _pos: std::io::SeekFrom) -> FsResult<u64> {
        todo!()
    }

    fn datasync_sync(self: Rc<Self>) -> FsResult<()> {
        todo!()
    }

    async fn datasync_async(self: Rc<Self>) -> FsResult<()> {
        todo!()
    }

    fn sync_sync(self: Rc<Self>) -> FsResult<()> {
        todo!()
    }

    async fn sync_async(self: Rc<Self>) -> FsResult<()> {
        todo!()
    }

    fn stat_sync(self: Rc<Self>) -> FsResult<FsStat> {
        todo!()
    }

    async fn stat_async(self: Rc<Self>) -> FsResult<FsStat> {
        todo!()
    }

    fn lock_sync(self: Rc<Self>, _exclusive: bool) -> FsResult<()> {
        todo!()
    }

    async fn lock_async(self: Rc<Self>, _exclusive: bool) -> FsResult<()> {
        todo!()
    }

    fn unlock_sync(self: Rc<Self>) -> FsResult<()> {
        todo!()
    }

    async fn unlock_async(self: Rc<Self>) -> FsResult<()> {
        todo!()
    }

    fn truncate_sync(self: Rc<Self>, _len: u64) -> FsResult<()> {
        todo!()
    }

    async fn truncate_async(self: Rc<Self>, _len: u64) -> FsResult<()> {
        todo!()
    }

    fn utime_sync(
        self: Rc<Self>,
        _atime_secs: i64,
        _atime_nanos: u32,
        _mtime_secs: i64,
        _mtime_nanos: u32,
    ) -> FsResult<()> {
        todo!()
    }

    async fn utime_async(
        self: Rc<Self>,
        _atime_secs: i64,
        _atime_nanos: u32,
        _mtime_secs: i64,
        _mtime_nanos: u32,
    ) -> FsResult<()> {
        todo!()
    }

    fn as_stdio(self: Rc<Self>) -> FsResult<std::process::Stdio> {
        todo!()
    }

    fn backing_fd(self: Rc<Self>) -> Option<ResourceHandleFd> {
        None
    }

    fn try_clone_inner(self: Rc<Self>) -> FsResult<Rc<dyn DenoFile>> {
        todo!()
    }
}
