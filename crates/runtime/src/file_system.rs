use std::path::{Path, PathBuf};
use std::rc::Rc;

use bytes::Bytes;
use deno_fs::{AccessCheckCb, FileSystem as DenoFileSystem, FsDirEntry, FsFileType, OpenOptions};
use deno_io::fs::{File, FsResult, FsStat};
use proven_store::Store;
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FsMetadata {
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub modified: (i64, u32), // seconds, nanos
    pub accessed: (i64, u32),
    pub created: (i64, u32),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Entry {
    File {
        content: Bytes,
        metadata: FsMetadata,
    },
    Directory {
        metadata: FsMetadata,
        children: Vec<String>,
    },
}

impl Entry {
    pub const fn metadata(&self) -> &FsMetadata {
        match self {
            Self::File { metadata, .. } | Self::Directory { metadata, .. } => metadata,
        }
    }

    pub const fn metadata_mut(&mut self) -> &mut FsMetadata {
        match self {
            Self::File { metadata, .. } | Self::Directory { metadata, .. } => metadata,
        }
    }
}

impl TryFrom<Bytes> for Entry {
    type Error = serde_json::Error;

    fn try_from(bytes: Bytes) -> Result<Self, Self::Error> {
        serde_json::from_slice(&bytes)
    }
}

impl TryFrom<Entry> for Bytes {
    type Error = serde_json::Error;

    fn try_from(entry: Entry) -> Result<Self, Self::Error> {
        serde_json::to_vec(&entry).map(Self::from)
    }
}

#[derive(Debug)]
pub struct FileSystem<S> {
    store: S,
    runtime: Handle,
}

impl<S> FileSystem<S>
where
    S: Store<Entry, serde_json::Error, serde_json::Error>,
{
    pub fn new(store: S) -> Self {
        Self {
            store,
            runtime: Handle::current(),
        }
    }

    fn normalize_path(path: &Path) -> String {
        path.components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("/")
    }

    async fn get_entry(&self, path: &Path) -> FsResult<Option<Entry>> {
        let key = Self::normalize_path(path);
        self.store.get(key).await.map_err(|_| todo!())
    }

    async fn put_entry(&self, path: &Path, entry: Entry) -> FsResult<()> {
        let key = Self::normalize_path(path);
        self.store.put(key, entry).await.map_err(|_| todo!())
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }
}

#[async_trait::async_trait(?Send)]
impl<S> DenoFileSystem for FileSystem<S>
where
    S: Store<Entry, serde_json::Error, serde_json::Error>,
{
    fn cwd(&self) -> FsResult<PathBuf> {
        todo!()
    }

    fn tmp_dir(&self) -> FsResult<PathBuf> {
        todo!()
    }

    fn chdir(&self, _path: &Path) -> FsResult<()> {
        todo!()
    }

    fn umask(&self, _mask: Option<u32>) -> FsResult<u32> {
        todo!()
    }

    fn open_sync(
        &self,
        path: &Path,
        options: OpenOptions,
        access_check: Option<AccessCheckCb>,
    ) -> FsResult<Rc<dyn File>> {
        let path = path.to_path_buf();
        let mut boxed_check = access_check.map(Box::new);
        let access_check = boxed_check.as_mut().map(|b| b as AccessCheckCb);
        self.block_on(self.open_async(path, options, access_check))
    }

    async fn open_async<'a>(
        &'a self,
        path: PathBuf,
        options: OpenOptions,
        _access_check: Option<AccessCheckCb<'a>>,
    ) -> FsResult<Rc<dyn File>> {
        let entry = if options.create {
            Entry::File {
                content: Bytes::new(),
                metadata: FsMetadata {
                    mode: 0o644,
                    uid: 0,
                    gid: 0,
                    modified: (0, 0),
                    accessed: (0, 0),
                    created: (0, 0),
                },
            }
        } else {
            self.get_entry(&path).await?.ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "File not found")
            })?
        };

        self.put_entry(&path, entry).await?;
        todo!("implement File wrapper")
    }

    fn mkdir_sync(&self, _path: &Path, _recursive: bool, _mode: Option<u32>) -> FsResult<()> {
        todo!()
    }

    async fn mkdir_async(
        &self,
        _path: PathBuf,
        _recursive: bool,
        _mode: Option<u32>,
    ) -> FsResult<()> {
        todo!()
    }

    async fn chmod_async(&self, _path: PathBuf, _mode: u32) -> FsResult<()> {
        todo!()
    }

    async fn chown_async(
        &self,
        _path: PathBuf,
        _uid: Option<u32>,
        _gid: Option<u32>,
    ) -> FsResult<()> {
        todo!()
    }

    fn chown_sync(&self, _path: &Path, _uid: Option<u32>, _gid: Option<u32>) -> FsResult<()> {
        todo!()
    }

    fn chmod_sync(&self, _path: &Path, _mode: u32) -> FsResult<()> {
        todo!()
    }

    fn lchown_sync(&self, _path: &Path, _uid: Option<u32>, _gid: Option<u32>) -> FsResult<()> {
        todo!()
    }

    async fn lchown_async(
        &self,
        _path: PathBuf,
        _uid: Option<u32>,
        _gid: Option<u32>,
    ) -> FsResult<()> {
        todo!()
    }

    fn remove_sync(&self, _path: &Path, _recursive: bool) -> FsResult<()> {
        todo!()
    }

    async fn remove_async(&self, _path: PathBuf, _recursive: bool) -> FsResult<()> {
        todo!()
    }

    fn copy_file_sync(&self, _oldpath: &Path, _newpath: &Path) -> FsResult<()> {
        todo!()
    }

    async fn copy_file_async(&self, _oldpath: PathBuf, _newpath: PathBuf) -> FsResult<()> {
        todo!()
    }

    fn cp_sync(&self, _path: &Path, _new_path: &Path) -> FsResult<()> {
        todo!()
    }

    async fn cp_async(&self, _path: PathBuf, _new_path: PathBuf) -> FsResult<()> {
        todo!()
    }

    fn stat_sync(&self, _path: &Path) -> FsResult<FsStat> {
        todo!()
    }

    async fn stat_async(&self, _path: PathBuf) -> FsResult<FsStat> {
        todo!()
    }

    fn lstat_sync(&self, _path: &Path) -> FsResult<FsStat> {
        todo!()
    }

    async fn lstat_async(&self, _path: PathBuf) -> FsResult<FsStat> {
        todo!()
    }

    fn realpath_sync(&self, _path: &Path) -> FsResult<PathBuf> {
        todo!()
    }

    async fn realpath_async(&self, _path: PathBuf) -> FsResult<PathBuf> {
        todo!()
    }

    fn read_dir_sync(&self, _path: &Path) -> FsResult<Vec<FsDirEntry>> {
        todo!()
    }

    async fn read_dir_async(&self, _path: PathBuf) -> FsResult<Vec<FsDirEntry>> {
        todo!()
    }

    fn rename_sync(&self, _oldpath: &Path, _newpath: &Path) -> FsResult<()> {
        todo!()
    }

    async fn rename_async(&self, _oldpath: PathBuf, _newpath: PathBuf) -> FsResult<()> {
        todo!()
    }

    fn link_sync(&self, _oldpath: &Path, _newpath: &Path) -> FsResult<()> {
        todo!()
    }

    async fn link_async(&self, _oldpath: PathBuf, _newpath: PathBuf) -> FsResult<()> {
        todo!()
    }

    fn symlink_sync(
        &self,
        _oldpath: &Path,
        _newpath: &Path,
        _file_type: Option<FsFileType>,
    ) -> FsResult<()> {
        todo!()
    }

    async fn symlink_async(
        &self,
        _oldpath: PathBuf,
        _newpath: PathBuf,
        _file_type: Option<FsFileType>,
    ) -> FsResult<()> {
        todo!()
    }

    fn read_link_sync(&self, _path: &Path) -> FsResult<PathBuf> {
        todo!()
    }

    async fn read_link_async(&self, _path: PathBuf) -> FsResult<PathBuf> {
        todo!()
    }

    fn truncate_sync(&self, _path: &Path, _len: u64) -> FsResult<()> {
        todo!()
    }

    async fn truncate_async(&self, _path: PathBuf, _len: u64) -> FsResult<()> {
        todo!()
    }

    fn utime_sync(
        &self,
        _path: &Path,
        _atime_secs: i64,
        _atime_nanos: u32,
        _mtime_secs: i64,
        _mtime_nanos: u32,
    ) -> FsResult<()> {
        todo!()
    }

    async fn utime_async(
        &self,
        _path: PathBuf,
        _atime_secs: i64,
        _atime_nanos: u32,
        _mtime_secs: i64,
        _mtime_nanos: u32,
    ) -> FsResult<()> {
        todo!()
    }

    fn lutime_sync(
        &self,
        _path: &Path,
        _atime_secs: i64,
        _atime_nanos: u32,
        _mtime_secs: i64,
        _mtime_nanos: u32,
    ) -> FsResult<()> {
        todo!()
    }

    async fn lutime_async(
        &self,
        _path: PathBuf,
        _atime_secs: i64,
        _atime_nanos: u32,
        _mtime_secs: i64,
        _mtime_nanos: u32,
    ) -> FsResult<()> {
        todo!()
    }
}
