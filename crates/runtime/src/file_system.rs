use std::path::{Path, PathBuf};
use std::rc::Rc;

use deno_fs::{AccessCheckCb, FileSystem as DenoFileSystem, FsDirEntry, FsFileType, OpenOptions};
use deno_io::fs::{File, FsResult, FsStat};

#[derive(Debug)]
pub struct FileSystem;

#[async_trait::async_trait(?Send)]
impl DenoFileSystem for FileSystem {
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
        _path: &Path,
        _options: OpenOptions,
        _access_check: Option<AccessCheckCb>,
    ) -> FsResult<Rc<dyn File>> {
        todo!()
    }

    async fn open_async<'a>(
        &'a self,
        _path: PathBuf,
        _options: OpenOptions,
        _access_check: Option<AccessCheckCb<'a>>,
    ) -> FsResult<Rc<dyn File>> {
        todo!()
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
