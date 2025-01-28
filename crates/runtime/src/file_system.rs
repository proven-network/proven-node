mod entry;
mod file;
mod metadata;

pub use entry::Entry;
use file::File;
use metadata::FsMetadata;

use std::path::{Path, PathBuf};
use std::rc::Rc;

use bytes::Bytes;
use deno_fs::{AccessCheckCb, FileSystem as DenoFileSystem, FsDirEntry, FsFileType, OpenOptions};
use deno_io::fs::{File as DenoFile, FsResult, FsStat};
use futures::executor::block_on;
use proven_store::Store;

#[derive(Debug)]
pub struct FileSystem<S> {
    store: S,
}

impl<S> FileSystem<S>
where
    S: Store<Entry, serde_json::Error, serde_json::Error>,
{
    pub const fn new(store: S) -> Self {
        Self { store }
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
            Entry::File(File {
                content: Bytes::new(),
                metadata: FsMetadata {
                    mode: options.mode.unwrap_or(0o644),
                    uid: 0,
                    gid: 0,
                    modified: (0, 0),
                    accessed: (0, 0),
                    created: (0, 0),
                },
            })
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

#[cfg(test)]
mod tests {
    use super::*;

    use proven_store_memory::MemoryStore;

    fn setup() -> FileSystem<MemoryStore<Entry, serde_json::Error, serde_json::Error>> {
        FileSystem::new(MemoryStore::new())
    }

    fn create_test_file() -> Entry {
        Entry::File(File {
            content: Bytes::from("test content"),
            metadata: FsMetadata {
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                modified: (0, 0),
                accessed: (0, 0),
                created: (0, 0),
            },
        })
    }

    #[test] // Change from tokio::test
    fn test_path_normalization() {
        assert_eq!(
            FileSystem::<MemoryStore<Entry, serde_json::Error, serde_json::Error>>::normalize_path(
                Path::new("/test/path")
            ),
            "test/path"
        );

        assert_eq!(
            FileSystem::<MemoryStore<Entry, serde_json::Error, serde_json::Error>>::normalize_path(
                Path::new("./test/../path")
            ),
            "test/../path"
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

            match (entry, retrieved) {
                (
                    Entry::File(File {
                        content: c1,
                        metadata: m1,
                    }),
                    Entry::File(File {
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

        // This will fail with todo!() until File implementation is complete
        assert!(result.is_err());

        // Verify the file was created in the store
        let entry = fs.get_entry(Path::new("/new.txt")).await.unwrap();
        assert!(matches!(entry, Some(Entry::File { .. })));
    }

    #[tokio::test]
    async fn test_open_with_create_and_mode() {
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

        // This will fail with todo!() until File implementation is complete
        assert!(result.is_err());

        // Verify the file was created with correct mode
        let entry = fs.get_entry(Path::new("/new.txt")).await.unwrap();
        if let Some(Entry::File(File { metadata, .. })) = entry {
            assert_eq!(metadata.mode, 0o755);
        } else {
            panic!("Expected file entry with mode 0o755");
        }
    }
}
