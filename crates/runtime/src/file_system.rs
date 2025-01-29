mod entry;
mod file;
mod metadata;

use entry::Entry;
pub use entry::StorageEntry;
use file::File;
use metadata::FsMetadata;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use bytes::Bytes;
use deno_fs::{AccessCheckCb, FileSystem as DenoFileSystem, FsDirEntry, FsFileType, OpenOptions};
use deno_io::fs::{File as DenoFile, FsResult, FsStat};
use futures::executor::block_on;
use proven_store::Store;

#[derive(Debug)]
pub struct FileSystem<S>
where
    S: Store<StorageEntry, serde_json::Error, serde_json::Error>,
{
    store: S,
}

impl<S> FileSystem<S>
where
    S: Store<StorageEntry, serde_json::Error, serde_json::Error>,
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

        normalized
            .iter()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("/")
            .trim_start_matches('/')
            .to_string()
    }

    async fn get_entry(&self, path: &Path) -> FsResult<Option<Entry<S>>> {
        let key = Self::normalize_path(path);
        self.store
            .get(key)
            .await
            .map(|opt| {
                opt.map(|storage| {
                    let mut entry: Entry<S> = storage.into();
                    if let Entry::File(ref mut file) = &mut entry {
                        file.path = path.to_path_buf();
                        file.store = Some(self.store.clone());
                    }
                    entry
                })
            })
            .map_err(|_| todo!())
    }

    async fn put_entry(&self, path: &Path, storage_entry: StorageEntry) -> FsResult<()> {
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
        let storage_entry = StorageEntry::Symlink {
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
        while let Some(entry) = self.get_entry(&path).await? {
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
}

#[async_trait::async_trait(?Send)]
impl<S> DenoFileSystem for FileSystem<S>
where
    S: Store<StorageEntry, serde_json::Error, serde_json::Error>,
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
        options: OpenOptions,
        _access_check: Option<AccessCheckCb<'a>>,
    ) -> FsResult<Rc<dyn DenoFile>> {
        let entry = if options.create {
            Entry::File(File::<S> {
                content: Bytes::new(),
                path: path.clone(),
                metadata: FsMetadata {
                    mode: options.mode.unwrap_or(0o644),
                    uid: 0,
                    gid: 0,
                    mtime: None,
                    atime: None,
                    birthtime: None,
                    ctime: None,
                },
                store: Some(self.store.clone()),
            })
        } else {
            self.get_entry(&path).await?.ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "File not found")
            })?
        };

        if options.create {
            self.put_entry(&path, entry.clone().into()).await?;
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

    async fn stat_async(&self, path: PathBuf) -> FsResult<FsStat> {
        let resolved = self.follow_symlinks(path).await?;
        self.lstat_async(resolved).await
    }

    fn lstat_sync(&self, path: &Path) -> FsResult<FsStat> {
        block_on(self.lstat_async(path.to_path_buf()))
    }

    async fn lstat_async(&self, path: PathBuf) -> FsResult<FsStat> {
        // Get stats without following symlinks
        (self.get_entry(&path).await?).map_or_else(
            || Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Not found").into()),
            |entry| {
                let metadata = entry.metadata();
                Ok(FsStat {
                    is_file: matches!(entry, Entry::File(_)),
                    is_directory: matches!(entry, Entry::Directory { .. }),
                    is_symlink: matches!(entry, Entry::Symlink { .. }),
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
            if let Some(entry) = self.get_entry(&child_path).await? {
                entries.push(FsDirEntry {
                    name,
                    is_file: matches!(entry, Entry::File(_)),
                    is_directory: matches!(entry, Entry::Directory { .. }),
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
        match self.get_entry(&path).await? {
            Some(Entry::Symlink { target, .. }) => Ok(PathBuf::from(target)),
            Some(_) => {
                Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Not a symlink").into())
            }
            None => Err(std::io::Error::new(std::io::ErrorKind::NotFound, "Not found").into()),
        }
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

#[cfg(test)]
mod tests {
    use super::{file::StorageFile, *};

    use proven_store_memory::MemoryStore;

    use crate::{ExecutionRequest, HandlerSpecifier, RuntimeOptions, Worker};

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

    fn setup() -> FileSystem<MemoryStore<StorageEntry, serde_json::Error, serde_json::Error>> {
        FileSystem::new(MemoryStore::new())
    }

    fn create_test_file() -> StorageEntry {
        StorageEntry::File(StorageFile {
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
        })
    }

    #[test]
    fn test_path_normalization() {
        assert_eq!(
            FileSystem::<MemoryStore<StorageEntry, serde_json::Error, serde_json::Error>>::normalize_path(
                Path::new("/test/path")
            ),
            "test/path"
        );

        assert_eq!(
            FileSystem::<MemoryStore<StorageEntry, serde_json::Error, serde_json::Error>>::normalize_path(
                Path::new("./test/../path")
            ),
            "path"
        );
    }

    #[test]
    fn test_get_nonexistent_entry() {
        let fs = setup();
        let result = block_on(fs.get_entry(Path::new("/nonexistent"))).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_put_and_get_entry() {
        let fs = setup();
        let path = Path::new("/test.txt");
        let entry = create_test_file();

        block_on(async {
            fs.put_entry(path, entry.clone()).await.unwrap();
            let retrieved = fs.get_entry(path).await.unwrap().unwrap();

            match (entry, retrieved.into()) {
                (
                    StorageEntry::File(StorageFile {
                        content: c1,
                        metadata: m1,
                    }),
                    StorageEntry::File(StorageFile {
                        content: c2,
                        metadata: m2,
                    }),
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

        // Verify the file was created in the store
        let entry = fs.get_entry(Path::new("/new.txt")).await.unwrap();
        assert!(matches!(entry, Some(Entry::File { .. })));
    }
}
