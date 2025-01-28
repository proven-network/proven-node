use super::metadata::FsMetadata;
use std::borrow::Cow;
use std::rc::Rc;

use bytes::Bytes;
use deno_core::{BufMutView, BufView, ResourceHandleFd};
use deno_io::fs::{File as DenoFile, FsResult, FsStat};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct File {
    pub content: Bytes,
    pub metadata: FsMetadata,
}

#[async_trait::async_trait(?Send)]
impl DenoFile for File {
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

    fn write_all_sync(self: Rc<Self>, _buf: &[u8]) -> FsResult<()> {
        todo!()
    }

    async fn write_all(self: Rc<Self>, _buf: BufView) -> FsResult<()> {
        todo!()
    }

    fn read_all_sync(self: Rc<Self>) -> FsResult<Cow<'static, [u8]>> {
        todo!()
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
