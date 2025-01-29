#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]

use super::metadata::FsMetadata;
use super::{FileSystem, StoredEntry};

use std::borrow::Cow;
use std::cell::RefCell;
use std::path::PathBuf;
use std::process::Stdio;
use std::rc::Rc;

use bytes::BytesMut;
use deno_core::{BufMutView, BufView, ResourceHandleFd, WriteOutcome};
use deno_fs::OpenOptions;
use deno_io::fs::{File as DenoFile, FsResult, FsStat};
use proven_store::Store;

/// Runtime file type with store reference
#[derive(Debug)]
pub struct File<S>
where
    S: Store<
        StoredEntry,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >,
{
    inner: RefCell<FileInner<S>>,
}

#[derive(Debug)]
pub struct FileInner<S>
where
    S: Store<
        StoredEntry,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >,
{
    content: BytesMut,
    path: PathBuf,
    metadata: FsMetadata,
    store: Option<S>,
    position: u64,
    append: bool,
    writable: bool,
    readable: bool,
}

impl<S> From<File<S>> for StoredEntry
where
    S: Store<Self, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
{
    fn from(file: File<S>) -> Self {
        let inner = file.inner.borrow();
        Self::File {
            metadata: inner.metadata.clone(),
            content: inner.content.clone().into(),
        }
    }
}

impl<S> File<S>
where
    S: Store<
        StoredEntry,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >,
{
    pub const fn new(
        content: BytesMut,
        path: PathBuf,
        metadata: FsMetadata,
        store: S,
        options: &OpenOptions,
    ) -> Self {
        Self {
            inner: RefCell::new(FileInner {
                content,
                path,
                metadata,
                store: Some(store),
                position: 0,
                append: options.append,
                writable: options.write,
                readable: options.read,
            }),
        }
    }

    pub fn content(&self) -> BytesMut {
        self.inner.borrow().content.clone()
    }

    pub fn metadata(&self) -> FsMetadata {
        self.inner.borrow().metadata.clone()
    }

    pub const fn metadata_mut(&self) -> &RefCell<FileInner<S>> {
        &self.inner
    }

    pub fn truncate(&self) {
        let mut inner = self.inner.borrow_mut();
        inner.content.clear();
        inner.position = 0;
    }

    fn write_at_position(&self, buf: &[u8]) -> usize {
        let mut inner = self.inner.borrow_mut();
        let write_pos = if inner.append {
            inner.content.len()
        } else {
            inner.position as usize
        };

        // For appending, we can use extend_from_slice
        if inner.append {
            inner.content.extend_from_slice(buf);
        } else {
            // Ensure we have enough capacity
            let required_len = write_pos + buf.len();
            if required_len > inner.content.len() {
                inner.content.resize(required_len, 0);
            }
            // Write directly into the buffer
            inner.content[write_pos..write_pos + buf.len()].copy_from_slice(buf);
        }

        // Update position
        let new_pos = (write_pos + buf.len()) as u64;
        inner.position = new_pos;
        buf.len()
    }

    #[allow(clippy::future_not_send)]
    async fn persist(&self) -> FsResult<()> {
        let (storage_entry, store, path) = {
            let inner = self.inner.borrow();
            let entry = StoredEntry::File {
                metadata: inner.metadata.clone(),
                content: inner.content.clone().into(),
            };
            let store = inner
                .store
                .as_ref()
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "Store is None"))?
                .clone();
            let path = inner.path.clone();
            (entry, store, path)
        };

        let key = FileSystem::<S>::normalize_path(&path);
        store
            .put(key, storage_entry)
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;

        Ok(())
    }

    pub fn with_content_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut BytesMut) -> R,
    {
        let mut inner = self.inner.borrow_mut();
        f(&mut inner.content)
    }

    pub fn with_metadata_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut FsMetadata) -> R,
    {
        let mut inner = self.inner.borrow_mut();
        f(&mut inner.metadata)
    }

    fn write_all_at_position(&self, buf: &[u8]) {
        let mut inner = self.inner.borrow_mut();
        if inner.append {
            inner.content.extend_from_slice(buf);
            inner.position = inner.content.len() as u64;
        } else {
            let write_pos = inner.position as usize;
            let required_len = write_pos + buf.len();
            if required_len > inner.content.len() {
                inner.content.resize(required_len, 0);
            }
            inner.content[write_pos..write_pos + buf.len()].copy_from_slice(buf);
            inner.position = (write_pos + buf.len()) as u64;
        }
    }

    fn read_at_position(&self, buf: &mut [u8]) -> FsResult<usize> {
        let mut inner = self.inner.borrow_mut();
        if !inner.readable {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "File not readable",
            )
            .into());
        }

        let available = inner.content.len().saturating_sub(inner.position as usize);
        if available == 0 {
            return Ok(0);
        }

        let to_read = buf.len().min(available);
        buf[..to_read].copy_from_slice(&inner.content[inner.position as usize..][..to_read]);
        inner.position += to_read as u64;

        Ok(to_read)
    }

    pub fn truncate_to(&self, len: u64) -> FsResult<()> {
        let mut inner = self.inner.borrow_mut();
        if !inner.writable {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "File not writable",
            )
            .into());
        }

        let new_len = len as usize;
        if new_len <= inner.content.len() {
            inner.content.truncate(new_len);
        } else {
            inner.content.resize(new_len, 0);
        }
        inner.position = inner.position.min(len);
        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl<S> DenoFile for File<S>
where
    S: Store<
            StoredEntry,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        > + 'static,
{
    fn read_sync(self: Rc<Self>, buf: &mut [u8]) -> FsResult<usize> {
        self.read_at_position(buf)
    }

    async fn read_byob(self: Rc<Self>, _buf: BufMutView) -> FsResult<(usize, BufMutView)> {
        todo!()
    }

    fn write_sync(self: Rc<Self>, buf: &[u8]) -> FsResult<usize> {
        {
            let inner = self.inner.borrow();
            if !inner.writable {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "File not writable",
                )
                .into());
            }
        }

        let bytes_written = self.write_at_position(buf);
        futures::executor::block_on(self.persist())?;
        Ok(bytes_written)
    }

    async fn write(self: Rc<Self>, buf: BufView) -> FsResult<WriteOutcome> {
        {
            let inner = self.inner.borrow();
            if !inner.writable {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "File not writable",
                )
                .into());
            }
        }

        let bytes_written = self.write_at_position(buf.as_ref());
        self.persist().await?;

        Ok(WriteOutcome::Full {
            nwritten: bytes_written,
        })
    }

    fn write_all_sync(self: Rc<Self>, buf: &[u8]) -> FsResult<()> {
        {
            let inner = self.inner.borrow();
            if !inner.writable {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "File not writable",
                )
                .into());
            }
        }

        self.write_all_at_position(buf);
        futures::executor::block_on(self.persist())?;
        Ok(())
    }

    async fn write_all(self: Rc<Self>, buf: BufView) -> FsResult<()> {
        {
            let inner = self.inner.borrow();
            if !inner.writable {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "File not writable",
                )
                .into());
            }
        }

        let bytes = buf.as_ref();
        self.write_all_at_position(bytes);
        self.persist().await?;

        Ok(())
    }

    fn read_all_sync(self: Rc<Self>) -> FsResult<Cow<'static, [u8]>> {
        Ok(Cow::Owned(self.inner.borrow().content.to_vec()))
    }

    async fn read_all_async(self: Rc<Self>) -> FsResult<Cow<'static, [u8]>> {
        Ok(Cow::Owned(self.inner.borrow().content.to_vec()))
    }

    fn chmod_sync(self: Rc<Self>, mode: u32) -> FsResult<()> {
        {
            let mut inner = self.inner.borrow_mut();
            inner.metadata.mode = mode;
        }
        futures::executor::block_on(self.persist())
    }

    async fn chmod_async(self: Rc<Self>, mode: u32) -> FsResult<()> {
        {
            let mut inner = self.inner.borrow_mut();
            inner.metadata.mode = mode;
        }
        self.persist().await
    }

    fn seek_sync(self: Rc<Self>, pos: std::io::SeekFrom) -> FsResult<u64> {
        let mut inner = self.inner.borrow_mut();
        let new_pos = match pos {
            std::io::SeekFrom::Start(p) => p,
            std::io::SeekFrom::End(p) => (inner.content.len() as i64 + p) as u64,
            std::io::SeekFrom::Current(p) => (inner.position as i64 + p) as u64,
        };

        inner.position = new_pos;
        Ok(new_pos)
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

    fn truncate_sync(self: Rc<Self>, len: u64) -> FsResult<()> {
        self.truncate_to(len)?;
        futures::executor::block_on(self.persist())
    }

    async fn truncate_async(self: Rc<Self>, len: u64) -> FsResult<()> {
        self.truncate_to(len)?;
        self.persist().await
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

    fn as_stdio(self: Rc<Self>) -> FsResult<Stdio> {
        todo!()
    }

    fn backing_fd(self: Rc<Self>) -> Option<ResourceHandleFd> {
        None
    }

    fn try_clone_inner(self: Rc<Self>) -> FsResult<Rc<dyn DenoFile>> {
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::file_system::FileSystem;
    use deno_fs::FileSystem as DenoFileSystem;
    use proven_store_memory::MemoryStore;
    use std::io::SeekFrom;

    fn setup() -> FileSystem<
        MemoryStore<
            crate::file_system::StoredEntry,
            ciborium::de::Error<std::io::Error>,
            ciborium::ser::Error<std::io::Error>,
        >,
    > {
        FileSystem::new(MemoryStore::new())
    }

    #[tokio::test]
    async fn test_write_and_read() {
        let fs = setup();
        let file = fs
            .open_async(
                PathBuf::from("/test.txt"),
                OpenOptions {
                    read: true,
                    write: true,
                    create: true,
                    truncate: false,
                    append: false,
                    create_new: false,
                    mode: Some(0o644),
                },
                None,
            )
            .await
            .unwrap();

        // Write some data
        let written = Rc::clone(&file).write_sync(b"Hello").unwrap();
        assert_eq!(written, 5);

        // Append more data
        file.write_sync(b", world!").unwrap();

        // Reopen to verify complete content
        let file = fs
            .open_async(
                PathBuf::from("/test.txt"),
                OpenOptions {
                    read: true,
                    write: false,
                    create: false,
                    truncate: false,
                    append: false,
                    create_new: false,
                    mode: None,
                },
                None,
            )
            .await
            .unwrap();

        let content = file.read_all_sync().unwrap();
        assert_eq!(&content[..], b"Hello, world!");
    }

    #[tokio::test]
    async fn test_append_mode() {
        let fs = setup();
        let file = fs
            .open_async(
                PathBuf::from("/append.txt"),
                OpenOptions {
                    read: true,
                    write: true,
                    create: true,
                    truncate: false,
                    append: true,
                    create_new: false,
                    mode: Some(0o644),
                },
                None,
            )
            .await
            .unwrap();

        Rc::clone(&file).write_all_sync(b"First").unwrap();
        file.write_all_sync(b"Second").unwrap();

        // Reopen to verify content
        let file = fs
            .open_async(
                PathBuf::from("/append.txt"),
                OpenOptions {
                    read: true,
                    write: false,
                    create: false,
                    truncate: false,
                    append: false,
                    create_new: false,
                    mode: None,
                },
                None,
            )
            .await
            .unwrap();

        let content = file.read_all_sync().unwrap();
        assert_eq!(&content[..], b"FirstSecond");
    }

    #[tokio::test]
    async fn test_seek() {
        let fs = setup();
        let file = fs
            .open_async(
                PathBuf::from("/seek.txt"),
                OpenOptions {
                    read: true,
                    write: true,
                    create: true,
                    truncate: false,
                    append: false,
                    create_new: false,
                    mode: Some(0o644),
                },
                None,
            )
            .await
            .unwrap();

        // Write initial content
        file.write_all_sync(b"Hello, world!").unwrap();

        // Reopen file to verify content
        let file = fs
            .open_async(
                PathBuf::from("/seek.txt"),
                OpenOptions {
                    read: true,
                    write: true,
                    create: false,
                    truncate: false,
                    append: false,
                    create_new: false,
                    mode: None,
                },
                None,
            )
            .await
            .unwrap();

        // Test seeking from start
        let pos = Rc::clone(&file).seek_sync(SeekFrom::Start(6)).unwrap();
        assert_eq!(pos, 6);

        // Read after seek
        let mut buf = [0u8; 7];
        let read = Rc::clone(&file).read_sync(&mut buf).unwrap();
        assert_eq!(read, 7);
        assert_eq!(&buf, b" world!");

        // Test seeking from end
        let pos = Rc::clone(&file).seek_sync(SeekFrom::End(-6)).unwrap();
        assert_eq!(pos, 7);

        // Test seeking from current
        let pos = Rc::clone(&file).seek_sync(SeekFrom::Current(-2)).unwrap();
        assert_eq!(pos, 5);

        // Verify final position by reading remaining content
        let mut buf = [0u8; 8];
        let read = file.read_sync(&mut buf).unwrap();
        assert_eq!(read, 8);
        assert_eq!(&buf, b", world!");
    }

    #[tokio::test]
    async fn test_permissions() {
        let fs = setup();

        // Create and test read-only file
        let readonly_file = fs
            .open_async(
                PathBuf::from("/readonly.txt"),
                OpenOptions {
                    read: true,
                    write: true, // Need write permission initially to set up test
                    create: true,
                    truncate: false,
                    append: false,
                    create_new: false,
                    mode: Some(0o444),
                },
                None,
            )
            .await
            .unwrap();

        // Write test data
        Rc::clone(&readonly_file)
            .write_all_sync(b"test content")
            .unwrap();

        // Reopen as read-only
        let readonly_file = fs
            .open_async(
                PathBuf::from("/readonly.txt"),
                OpenOptions {
                    read: true,
                    write: false,
                    create: false,
                    truncate: false,
                    append: false,
                    create_new: false,
                    mode: None,
                },
                None,
            )
            .await
            .unwrap();

        // Write should fail but read should succeed
        assert!(Rc::clone(&readonly_file).write_sync(b"test").is_err());
        assert!(Rc::clone(&readonly_file).write_all_sync(b"test").is_err());

        // Read the content and compare as slices
        let content = Rc::clone(&readonly_file).read_all_sync().unwrap();
        assert_eq!(&content[..], b"test content");

        // Create and test write-only file
        let writeonly_file = fs
            .open_async(
                PathBuf::from("/writeonly.txt"),
                OpenOptions {
                    read: false,
                    write: true,
                    create: true,
                    truncate: false,
                    append: false,
                    create_new: false,
                    mode: Some(0o222),
                },
                None,
            )
            .await
            .unwrap();

        // Write should succeed
        assert!(Rc::clone(&writeonly_file).write_sync(b"test").is_ok());

        // Verify content by reopening with read permissions
        let readable_file = fs
            .open_async(
                PathBuf::from("/writeonly.txt"),
                OpenOptions {
                    read: true,
                    write: false,
                    create: false,
                    truncate: false,
                    append: false,
                    create_new: false,
                    mode: None,
                },
                None,
            )
            .await
            .unwrap();

        let content = readable_file.read_all_sync().unwrap();
        assert_eq!(&content[..], b"test");

        // Verify original file still can't read
        let mut buf = [0u8; 4];
        assert!(writeonly_file.read_sync(&mut buf).is_err());
    }
}
