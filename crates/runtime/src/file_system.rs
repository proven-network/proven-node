#![allow(clippy::cast_possible_truncation)]

mod entry;
mod file;
mod metadata;
mod stored_entry;

use entry::Entry;
use file::File;
use metadata::FsMetadata;
pub use stored_entry::StoredEntry;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use bytes::{Bytes, BytesMut};
use deno_fs::{AccessCheckCb, FileSystem as DenoFileSystem, FsDirEntry, FsFileType, OpenOptions};
use deno_io::fs::{File as DenoFile, FsResult, FsStat};
use futures::executor::block_on;
use proven_store::Store;

#[derive(Debug)]
pub struct FileSystem<S>
where
    S: Store<
        StoredEntry,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >,
{
    store: S,
}

impl<S> FileSystem<S>
where
    S: Store<
        StoredEntry,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >,
{
    pub const fn new(store: S) -> Self {
        Self { store }
    }

    fn normalize_path(path: &Path) -> String {
        let mut normalized = Vec::new();

        for component in path.components() {
            match component {
                std::path::Component::ParentDir => {
                    if let Some(std::path::Component::Normal(_)) = normalized.last() {
                        normalized.pop();
                    } else {
                        normalized.push(component);
                    }
                }
                std::path::Component::CurDir => {}
                _ => normalized.push(component),
            }
        }

        // Always strip leading slashes for consistency
        normalized
            .iter()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("/")
            .trim_matches('/')
            .to_string()
    }

    async fn get_entry(
        &self,
        path: &Path,
        open_options: &OpenOptions,
    ) -> FsResult<Option<Entry<S>>> {
        self.get_stored_entry(path)
            .await
            .map(|opt| {
                opt.map(|stored_entry| match stored_entry {
                    StoredEntry::Directory { metadata } => Entry::Directory { metadata },
                    StoredEntry::File { content, metadata } => Entry::File(File::new(
                        content.into(),
                        path.to_path_buf(),
                        metadata,
                        self.store.clone(),
                        open_options,
                    )),
                    StoredEntry::Symlink { metadata, target } => {
                        Entry::Symlink { metadata, target }
                    }
                })
            })
            .map_err(|_| todo!())
    }

    async fn get_stored_entry(&self, path: &Path) -> FsResult<Option<StoredEntry>> {
        let key = Self::normalize_path(path);

        self.store.get(key).await.map_err(|_| todo!())
    }

    async fn put_entry(&self, path: &Path, storage_entry: StoredEntry) -> FsResult<()> {
        let key = Self::normalize_path(path);
        self.store
            .put(key, storage_entry)
            .await
            .map_err(|_| todo!())
    }

    async fn list_directory(&self, path: &Path) -> FsResult<Vec<String>> {
        let prefix = Self::normalize_path(path);
        let prefix = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };

        let keys = self
            .store
            .keys_with_prefix(&prefix)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        // Extract just the immediate children
        Ok(keys
            .into_iter()
            .filter_map(|key| {
                let remainder = key.strip_prefix(&prefix)?;
                let name = remainder.split('/').next()?;
                if name.is_empty() {
                    None
                } else {
                    Some(name.to_string())
                }
            })
            .collect())
    }

    async fn create_symlink(
        &self,
        oldpath: &Path,
        newpath: &Path,
        _file_type: Option<FsFileType>,
    ) -> FsResult<()> {
        let target = Self::normalize_path(oldpath);
        let storage_entry = StoredEntry::Symlink {
            metadata: FsMetadata {
                mode: 0o777,
                uid: 0,
                gid: 0,
                mtime: None,
                atime: None,
                birthtime: None,
                ctime: None,
            },
            target,
        };
        self.put_entry(newpath, storage_entry).await
    }

    async fn follow_symlinks(&self, mut path: PathBuf) -> FsResult<PathBuf> {
        let mut seen = HashSet::new();
        while let Some(entry) = self.get_stored_entry(&path).await? {
            if let Some(target) = entry.symlink_target() {
                if !seen.insert(path.clone()) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Symlink loop detected",
                    )
                    .into());
                }
                path = PathBuf::from(target);
            } else {
                break;
            }
        }
        Ok(path)
    }

    async fn chmod(&self, path: &Path, mode: u32) -> FsResult<()> {
        let entry = self
            .get_stored_entry(path)
            .await?
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "Not found"))?;

        let updated_entry = match entry {
            StoredEntry::Directory { mut metadata } => {
                metadata.mode = mode;
                StoredEntry::Directory { metadata }
            }
            StoredEntry::File {
                content,
                mut metadata,
            } => {
                metadata.mode = mode;
                StoredEntry::File { content, metadata }
            }
            StoredEntry::Symlink {
                mut metadata,
                target,
            } => {
                metadata.mode = mode;
                StoredEntry::Symlink { metadata, target }
            }
        };

        self.put_entry(path, updated_entry).await
    }

    async fn truncate(&self, path: &Path, len: u64) -> FsResult<()> {
        let entry = self
            .get_stored_entry(path)
            .await?
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "Not found"))?;

        match entry {
            StoredEntry::File {
                mut content,
                metadata,
            } => {
                let new_len = len as usize;
                if new_len <= content.len() {
                    content = content.slice(0..new_len);
                } else {
                    let mut new_content = BytesMut::with_capacity(new_len);
                    new_content.extend_from_slice(&content);
                    new_content.resize(new_len, 0);
                    content = new_content.freeze();
                }
                self.put_entry(path, StoredEntry::File { content, metadata })
                    .await
            }
            _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Not a file").into()),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl<S> DenoFileSystem for FileSystem<S>
where
    S: Store<
        StoredEntry,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >,
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
    ) -> FsResult<Rc<dyn DenoFile>> {
        let path = path.to_path_buf();
        let mut boxed_check = access_check.map(Box::new);
        let access_check = boxed_check.as_mut().map(|b| b as AccessCheckCb);
        block_on(self.open_async(path, options, access_check))
    }

    async fn open_async<'a>(
        &'a self,
        path: PathBuf,
        open_options: OpenOptions,
        _access_check: Option<AccessCheckCb<'a>>,
    ) -> FsResult<Rc<dyn DenoFile>> {
        let normalized_path = PathBuf::from(Self::normalize_path(&path));

        let entry = if let Some(entry) = self.get_entry(&normalized_path, &open_options).await? {
            if open_options.create_new {
                return Err(
                    std::io::Error::new(std::io::ErrorKind::AlreadyExists, "File exists").into(),
                );
            }

            entry
        } else if open_options.create {
            let entry = Entry::File(File::new(
                BytesMut::new(),
                normalized_path.clone(),
                FsMetadata {
                    mode: open_options.mode.unwrap_or(0o644),
                    uid: 0,
                    gid: 0,
                    mtime: None,
                    atime: None,
                    birthtime: None,
                    ctime: None,
                },
                self.store.clone(),
                &open_options,
            ));

            // Persist empty file to store before handing back File
            self.put_entry(
                &normalized_path,
                StoredEntry::File {
                    metadata: entry.metadata(),
                    content: Bytes::new(),
                },
            )
            .await?;

            entry
        } else {
            return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "File not found").into());
        };

        // Handle truncation
        if open_options.truncate {
            if let Entry::File(file) = &entry {
                file.truncate();
            }
        }

        match entry {
            Entry::File(file) => Ok(Rc::new(file)),
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "Not a file").into()),
        }
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

    async fn chmod_async(&self, path: PathBuf, mode: u32) -> FsResult<()> {
        self.chmod(&path, mode).await
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

    fn chmod_sync(&self, path: &Path, mode: u32) -> FsResult<()> {
        block_on(self.chmod(path, mode))
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

    async fn stat_async(&self, path: PathBuf) -> FsResult<FsStat> {
        let resolved = self.follow_symlinks(path).await?;
        self.lstat_async(resolved).await
    }

    fn lstat_sync(&self, path: &Path) -> FsResult<FsStat> {
        block_on(self.lstat_async(path.to_path_buf()))
    }

    async fn lstat_async(&self, path: PathBuf) -> FsResult<FsStat> {
        // Get stats without following symlinks
        (self.get_stored_entry(&path).await?).map_or_else(
            || Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Not found").into()),
            |stored_entry| {
                let metadata = stored_entry.metadata();
                Ok(FsStat {
                    is_directory: matches!(stored_entry, StoredEntry::Directory { .. }),
                    is_file: matches!(stored_entry, StoredEntry::File { .. }),
                    is_symlink: matches!(stored_entry, StoredEntry::Symlink { .. }),
                    mode: metadata.mode,
                    uid: metadata.uid,
                    gid: metadata.gid,
                    size: 0, // TODO: Implement this
                    atime: metadata.atime,
                    mtime: metadata.mtime,
                    birthtime: metadata.birthtime,
                    ctime: metadata.ctime,
                    blksize: 4096,
                    blocks: 0, // TODO: Implement this
                    is_block_device: false,
                    is_char_device: false,
                    is_fifo: false,
                    is_socket: false,
                    dev: 0,
                    ino: 0,
                    rdev: 0,
                    nlink: 1,
                })
            },
        )
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

    async fn read_dir_async(&self, path: PathBuf) -> FsResult<Vec<FsDirEntry>> {
        let children = self.list_directory(&path).await?;

        let mut entries = Vec::new();
        for name in children {
            let child_path = path.join(&name);
            if let Some(stored_entry) = self.get_stored_entry(&child_path).await? {
                entries.push(FsDirEntry {
                    name,
                    is_file: matches!(stored_entry, StoredEntry::File { .. }),
                    is_directory: matches!(stored_entry, StoredEntry::Directory { .. }),
                    is_symlink: false,
                });
            }
        }

        Ok(entries)
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
        oldpath: &Path,
        newpath: &Path,
        file_type: Option<FsFileType>,
    ) -> FsResult<()> {
        block_on(self.symlink_async(oldpath.to_path_buf(), newpath.to_path_buf(), file_type))
    }

    async fn symlink_async(
        &self,
        oldpath: PathBuf,
        newpath: PathBuf,
        file_type: Option<FsFileType>,
    ) -> FsResult<()> {
        self.create_symlink(&oldpath, &newpath, file_type).await
    }

    fn read_link_sync(&self, path: &Path) -> FsResult<PathBuf> {
        block_on(self.read_link_async(path.to_path_buf()))
    }

    async fn read_link_async(&self, path: PathBuf) -> FsResult<PathBuf> {
        match self.get_stored_entry(&path).await? {
            Some(StoredEntry::Symlink { target, .. }) => Ok(PathBuf::from(target)),
            Some(_) => {
                Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Not a symlink").into())
            }
            None => Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Not found").into()),
        }
    }

    fn truncate_sync(&self, path: &Path, len: u64) -> FsResult<()> {
        block_on(self.truncate(path, len))
    }

    async fn truncate_async(&self, path: PathBuf, len: u64) -> FsResult<()> {
        self.truncate(&path, len).await
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{ExecutionRequest, HandlerSpecifier, RuntimeOptions, Worker};
    use bytes::Bytes;
    use proven_store_memory::MemoryStore;

    #[tokio::test]
    async fn test_read_write() {
        let runtime_options = RuntimeOptions::for_test_code("file_system/test_read_write");
        let mut worker = Worker::new(runtime_options).await.unwrap();

        let request = ExecutionRequest::Rpc {
            accounts: vec![],
            args: vec![],
            dapp_definition_address: "dapp_definition_address".to_string(),
            handler_specifier: HandlerSpecifier::parse("file:///main.ts#test").unwrap(),
            identity: "my_identity".to_string(),
        };

        let result = worker.execute(request).await;

        if let Err(err) = result {
            panic!("Error: {err:?}");
        }

        let execution_result = result.unwrap();

        assert!(execution_result.output.is_string());
        assert_eq!(execution_result.output.as_str().unwrap(), "Hello, world!");
    }

    fn setup() -> FileSystem<
        MemoryStore<
            StoredEntry,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
    > {
        FileSystem::new(MemoryStore::new())
    }

    fn create_test_file() -> StoredEntry {
        StoredEntry::File {
            content: Bytes::from("test content"),
            metadata: FsMetadata {
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                mtime: None,
                atime: None,
                birthtime: None,
                ctime: None,
            },
        }
    }

    #[test]
    fn test_path_normalization() {
        assert_eq!(
            FileSystem::<
                MemoryStore<
                    StoredEntry,
                    ciborium::de::Error<std::io::Error>,
                    ciborium::ser::Error<std::io::Error>,
                >,
            >::normalize_path(Path::new("/test/path")),
            "test/path"
        );

        assert_eq!(
            FileSystem::<
                MemoryStore<
                    StoredEntry,
                    ciborium::de::Error<std::io::Error>,
                    ciborium::ser::Error<std::io::Error>,
                >,
            >::normalize_path(Path::new("./test/../path")),
            "path"
        );
    }

    #[test]
    fn test_get_nonexistent_entry() {
        let fs = setup();
        let result = block_on(fs.get_stored_entry(Path::new("/nonexistent"))).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_put_and_get_entry() {
        let fs = setup();
        let path = Path::new("/test.txt");
        let entry = create_test_file();

        block_on(async {
            fs.put_entry(path, entry.clone()).await.unwrap();
            let retrieved = fs.get_stored_entry(path).await.unwrap().unwrap();

            match (entry, retrieved) {
                (
                    StoredEntry::File {
                        content: c1,
                        metadata: m1,
                    },
                    StoredEntry::File {
                        content: c2,
                        metadata: m2,
                    },
                ) => {
                    assert_eq!(c1, c2);
                    assert_eq!(m1.mode, m2.mode);
                }
                _ => panic!("Expected file entries"),
            }
        });
    }

    #[tokio::test]
    async fn test_open_nonexistent_file() {
        let fs = setup();
        let result = fs
            .open_async(
                PathBuf::from("/nonexistent.txt"),
                OpenOptions {
                    read: true,
                    write: false,
                    create: false,
                    truncate: false,
                    append: false,
                    create_new: false,
                    mode: Some(0o755),
                },
                None,
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_open_with_create() {
        let fs = setup();
        let result = fs
            .open_async(
                PathBuf::from("/new.txt"),
                OpenOptions {
                    read: true,
                    write: true,
                    create: true,
                    truncate: false,
                    append: false,
                    create_new: false,
                    mode: Some(0o755),
                },
                None,
            )
            .await;

        assert!(result.is_ok());

        let entry = fs.get_stored_entry(Path::new("/new.txt")).await.unwrap();
        assert!(matches!(entry, Some(StoredEntry::File { .. })));
    }
}
