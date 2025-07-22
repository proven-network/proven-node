//! FUSE filesystem implementation for VSOCK-FUSE
//!
//! This module provides the POSIX-compliant filesystem interface.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType as FuseFileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow,
};
use parking_lot::RwLock;
use proven_logger::{debug, error, info};
use tokio::sync::{mpsc, oneshot};

use crate::{
    DirectoryId, FileId, FileMetadata, FileType as VsockFileType,
    config::Config,
    database::{DatabaseCachePolicy, DatabaseDetector, LockManager},
    encryption::EncryptionLayer,
    fuse_async::FuseOperation,
    metadata::LocalEncryptedMetadataStore,
    storage::BlobStorage,
};

/// FUSE filesystem implementation
pub struct VsockFuseFs {
    /// Configuration
    config: Arc<Config>,
    /// Channel to send operations to async handler
    operation_tx: mpsc::UnboundedSender<FuseOperation>,
    /// Inode to FileId mapping
    inode_map: Arc<RwLock<InodeMap>>,
    /// Open file handles
    file_handles: Arc<RwLock<HashMap<u64, FileHandle>>>,
    /// Next file handle ID
    next_handle: Arc<RwLock<u64>>,
    /// Lock manager for file locking
    lock_manager: Arc<LockManager>,
    /// Database file detector
    db_detector: Arc<DatabaseDetector>,
}

/// Inode mapping for filesystem operations
struct InodeMap {
    /// FileId to inode mapping
    file_to_inode: HashMap<FileId, u64>,
    /// Inode to FileId mapping
    inode_to_file: HashMap<u64, FileId>,
    /// Next available inode
    next_inode: u64,
}

impl InodeMap {
    fn new() -> Self {
        let mut map = Self {
            file_to_inode: HashMap::new(),
            inode_to_file: HashMap::new(),
            next_inode: 2, // Start at 2, 1 is reserved for root
        };

        // Add root directory mapping
        let root_id = FileId::from_bytes([0; 32]);
        map.file_to_inode.insert(root_id, 1);
        map.inode_to_file.insert(1, root_id);

        map
    }

    fn get_or_create_inode(&mut self, file_id: FileId) -> u64 {
        if let Some(&inode) = self.file_to_inode.get(&file_id) {
            inode
        } else {
            let inode = self.next_inode;
            self.next_inode += 1;
            self.file_to_inode.insert(file_id, inode);
            self.inode_to_file.insert(inode, file_id);
            inode
        }
    }

    fn get_file_id(&self, inode: u64) -> Option<FileId> {
        self.inode_to_file.get(&inode).copied()
    }
}

/// Open file handle information
struct FileHandle {
    /// File ID
    file_id: FileId,
    /// Open flags
    _flags: i32,
    /// Current position in file
    _position: u64,
    /// Write buffer for batching
    _write_buffer: Vec<u8>,
    /// Whether file was opened for writing
    write_mode: bool,
    /// Database cache policy for this file
    db_policy: DatabaseCachePolicy,
    /// Whether this is a direct I/O file
    _direct_io: bool,
}

impl VsockFuseFs {
    /// Create a new FUSE filesystem instance with async handler
    pub fn new(
        config: Arc<Config>,
        _crypto: Arc<EncryptionLayer>,
        _metadata: Arc<LocalEncryptedMetadataStore>,
        _storage: Arc<dyn BlobStorage>,
    ) -> (Self, mpsc::UnboundedReceiver<FuseOperation>) {
        let db_detector = Arc::new(DatabaseDetector::new(
            config.filesystem.database_patterns.clone(),
        ));

        // Create channel for async operations
        let (tx, rx) = mpsc::unbounded_channel();

        let fs = Self {
            config,
            operation_tx: tx,
            inode_map: Arc::new(RwLock::new(InodeMap::new())),
            file_handles: Arc::new(RwLock::new(HashMap::new())),
            next_handle: Arc::new(RwLock::new(1)),
            lock_manager: Arc::new(LockManager::new()),
            db_detector,
        };

        (fs, rx)
    }

    /// Convert internal file type to FUSE file type
    fn to_fuse_file_type(file_type: VsockFileType) -> FuseFileType {
        match file_type {
            VsockFileType::Regular => FuseFileType::RegularFile,
            VsockFileType::Directory => FuseFileType::Directory,
            VsockFileType::Symlink => FuseFileType::Symlink,
        }
    }

    /// Convert metadata to FUSE file attributes
    fn metadata_to_attr(&self, metadata: &FileMetadata, inode: u64) -> FileAttr {
        FileAttr {
            ino: inode,
            size: metadata.size,
            blocks: metadata.size.div_ceil(512),
            atime: metadata.accessed_at,
            mtime: metadata.modified_at,
            ctime: metadata.modified_at,
            crtime: metadata.created_at,
            kind: Self::to_fuse_file_type(metadata.file_type),
            perm: metadata.permissions as u16,
            nlink: metadata.nlink,
            uid: metadata.uid,
            gid: metadata.gid,
            rdev: 0,
            blksize: self.config.filesystem.block_size as u32,
            flags: 0,
        }
    }

    /// Get the next file handle ID
    fn next_file_handle(&self) -> u64 {
        let mut next = self.next_handle.write();
        let handle = *next;
        *next += 1;
        handle
    }
}

impl Filesystem for VsockFuseFs {
    fn init(
        &mut self,
        _req: &Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        info!("FUSE filesystem init called");
        Ok(())
    }

    fn destroy(&mut self) {
        info!("FUSE filesystem destroy called");
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        // Get storage statistics
        let (tx, rx) = oneshot::channel();
        if self
            .operation_tx
            .send(FuseOperation::GetStats { reply: tx })
            .is_err()
        {
            reply.error(libc::EIO);
            return;
        }

        match rx.blocking_recv() {
            Ok(Ok(stats)) => {
                let total_blocks =
                    stats.hot_tier.total_bytes / self.config.filesystem.block_size as u64;
                let free_blocks = (stats.hot_tier.total_bytes - stats.hot_tier.used_bytes)
                    / self.config.filesystem.block_size as u64;
                let available_blocks = free_blocks * 95 / 100; // Reserve 5%

                reply.statfs(
                    total_blocks,                             // total blocks
                    free_blocks,                              // free blocks
                    available_blocks,                         // available blocks
                    stats.hot_tier.file_count,                // total files
                    free_blocks,                              // free files (estimate)
                    self.config.filesystem.block_size as u32, // block size
                    255,                                      // max name length
                    self.config.filesystem.block_size as u32, // fragment size
                );
            }
            _ => reply.error(libc::EIO),
        }
    }

    fn access(&mut self, _req: &Request, ino: u64, _mask: i32, reply: ReplyEmpty) {
        // For now, always allow access
        // In a real implementation, we would check permissions
        if ino == 1 || self.inode_map.read().get_file_id(ino).is_some() {
            reply.ok();
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!("FUSE lookup: parent={parent}, name={name:?}");
        let name_bytes = name.as_bytes().to_vec();

        // Get parent directory ID
        let parent_id = match self.inode_map.read().get_file_id(parent) {
            Some(id) => DirectoryId(id),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Send lookup request through channel
        let (tx, rx) = oneshot::channel();
        info!("Sending Lookup operation to async handler");
        if self
            .operation_tx
            .send(FuseOperation::Lookup {
                parent_id,
                name: name_bytes.clone(),
                reply: tx,
            })
            .is_err()
        {
            error!("Failed to send Lookup operation");
            reply.error(libc::EIO);
            return;
        }
        info!("Lookup operation sent, waiting for response");

        // Wait for response
        match rx.blocking_recv() {
            Ok(Ok(Some(entry))) => {
                // Get or create inode for this file
                let inode = self.inode_map.write().get_or_create_inode(entry.file_id);

                // Get full metadata
                let (tx, rx) = oneshot::channel();
                if self
                    .operation_tx
                    .send(FuseOperation::GetMetadata {
                        file_id: entry.file_id,
                        reply: tx,
                    })
                    .is_err()
                {
                    reply.error(libc::EIO);
                    return;
                }

                match rx.blocking_recv() {
                    Ok(Ok(Some(metadata))) => {
                        let attr = self.metadata_to_attr(&metadata, inode);
                        let ttl = self.config.cache.attr_timeout;
                        reply.entry(&ttl, &attr, 0);
                    }
                    _ => reply.error(libc::EIO),
                }
            }
            Ok(Ok(None)) => reply.error(libc::ENOENT),
            _ => reply.error(libc::EIO),
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        // Special case for root
        if ino == 1 {
            let attr = FileAttr {
                ino: 1,
                size: 4096,
                blocks: 8,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: FuseFileType::Directory,
                perm: 0o755,
                nlink: 2,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 4096,
                flags: 0,
            };
            reply.attr(&self.config.cache.attr_timeout, &attr);
            return;
        }

        // Get file ID from inode
        let file_id = match self.inode_map.read().get_file_id(ino) {
            Some(id) => id,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Send get metadata request
        let (tx, rx) = oneshot::channel();
        if self
            .operation_tx
            .send(FuseOperation::GetMetadata { file_id, reply: tx })
            .is_err()
        {
            reply.error(libc::EIO);
            return;
        }

        // Wait for response
        match rx.blocking_recv() {
            Ok(Ok(Some(metadata))) => {
                let attr = self.metadata_to_attr(&metadata, ino);
                reply.attr(&self.config.cache.attr_timeout, &attr);
            }
            Ok(Ok(None)) => reply.error(libc::ENOENT),
            _ => reply.error(libc::EIO),
        }
    }

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        // Get file ID
        let file_id = match self.inode_map.read().get_file_id(ino) {
            Some(id) => id,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Get current metadata
        let (tx, rx) = oneshot::channel();
        if self
            .operation_tx
            .send(FuseOperation::GetMetadata { file_id, reply: tx })
            .is_err()
        {
            reply.error(libc::EIO);
            return;
        }

        let mut metadata = match rx.blocking_recv() {
            Ok(Ok(Some(m))) => m,
            _ => {
                reply.error(libc::EIO);
                return;
            }
        };

        // Apply changes
        if let Some(m) = mode {
            metadata.permissions = m;
        }
        if let Some(u) = uid {
            metadata.uid = u;
        }
        if let Some(g) = gid {
            metadata.gid = g;
        }
        if let Some(s) = size {
            // Handle truncation
            metadata.size = s;
            // TODO: Actually truncate the data blocks
        }

        let now = SystemTime::now();
        if let Some(t) = atime {
            metadata.accessed_at = match t {
                TimeOrNow::SpecificTime(time) => time,
                TimeOrNow::Now => now,
            };
        }
        if let Some(t) = mtime {
            metadata.modified_at = match t {
                TimeOrNow::SpecificTime(time) => time,
                TimeOrNow::Now => now,
            };
        }

        // Update metadata
        let (tx, rx) = oneshot::channel();
        if self
            .operation_tx
            .send(FuseOperation::UpdateMetadata {
                file_id,
                metadata: metadata.clone(),
                reply: tx,
            })
            .is_err()
        {
            reply.error(libc::EIO);
            return;
        }

        match rx.blocking_recv() {
            Ok(Ok(_)) => {
                let attr = self.metadata_to_attr(&metadata, ino);
                reply.attr(&self.config.cache.attr_timeout, &attr);
            }
            _ => reply.error(libc::EIO),
        }
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        // Get file ID
        let file_id = match self.inode_map.read().get_file_id(ino) {
            Some(id) => id,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Check if file exists
        let (tx, rx) = oneshot::channel();
        if self
            .operation_tx
            .send(FuseOperation::GetMetadata { file_id, reply: tx })
            .is_err()
        {
            reply.error(libc::EIO);
            return;
        }

        match rx.blocking_recv() {
            Ok(Ok(Some(_metadata))) => {
                // Get the file path from metadata
                // For now, just use a default path - in production this would decrypt the name
                let file_path = std::path::PathBuf::from("unknown");

                // Determine database cache policy
                let db_policy = DatabaseCachePolicy::for_file(&file_path, &self.db_detector);
                let direct_io = self.config.filesystem.database_direct_io
                    && db_policy.write_mode() == crate::database::DatabaseWriteMode::DirectIO;

                // Create file handle
                let handle_id = self.next_file_handle();
                let handle = FileHandle {
                    file_id,
                    _flags: flags,
                    _position: 0,
                    _write_buffer: Vec::new(),
                    write_mode: (flags & libc::O_WRONLY != 0) || (flags & libc::O_RDWR != 0),
                    db_policy: db_policy.clone(),
                    _direct_io: direct_io,
                };

                self.file_handles.write().insert(handle_id, handle);

                // Set open flags
                let mut open_flags = 0;
                if direct_io {
                    open_flags |= fuser::consts::FOPEN_DIRECT_IO;
                }

                reply.opened(handle_id, open_flags);
            }
            Ok(Ok(None)) => reply.error(libc::ENOENT),
            _ => reply.error(libc::EIO),
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        // Get file handle
        let file_id = match self.file_handles.read().get(&fh) {
            Some(handle) => handle.file_id,
            None => {
                reply.error(libc::EBADF);
                return;
            }
        };

        // Send read request
        let (tx, rx) = oneshot::channel();
        if self
            .operation_tx
            .send(FuseOperation::ReadFile {
                file_id,
                offset: offset as u64,
                size,
                reply: tx,
            })
            .is_err()
        {
            reply.error(libc::EIO);
            return;
        }

        // Wait for response
        match rx.blocking_recv() {
            Ok(Ok(data)) => reply.data(&data),
            _ => reply.error(libc::EIO),
        }
    }

    fn write(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        // Get file handle
        let (file_id, write_mode, db_policy) = match self.file_handles.read().get(&fh) {
            Some(handle) => (handle.file_id, handle.write_mode, handle.db_policy.clone()),
            None => {
                reply.error(libc::EBADF);
                return;
            }
        };

        if !write_mode {
            reply.error(libc::EBADF);
            return;
        }

        // Send write request
        let (tx, rx) = oneshot::channel();
        if self
            .operation_tx
            .send(FuseOperation::WriteFile {
                file_id,
                offset: offset as u64,
                data: data.to_vec(),
                reply: tx,
            })
            .is_err()
        {
            reply.error(libc::EIO);
            return;
        }

        // Wait for response
        match rx.blocking_recv() {
            Ok(Ok(written)) => {
                // For journal files, ensure immediate persistence
                if db_policy.write_mode() == crate::database::DatabaseWriteMode::WriteThrough {
                    // Send flush request
                    let (tx, rx) = oneshot::channel();
                    let _ = self
                        .operation_tx
                        .send(FuseOperation::FlushFile { file_id, reply: tx });
                    let _ = rx.blocking_recv();
                }

                reply.written(written as u32);
            }
            _ => reply.error(libc::EIO),
        }
    }

    fn release(
        &mut self,
        req: &Request,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        // Get file ID before removing handle
        let file_id = self.file_handles.read().get(&fh).map(|h| h.file_id);

        // Remove file handle
        self.file_handles.write().remove(&fh);

        // Clean up any locks held by this process on this file
        if let Some(_file_id) = file_id {
            self.lock_manager.remove_process_locks(req.pid());
        }

        reply.ok();
    }

    fn fsync(&mut self, _req: &Request, _ino: u64, fh: u64, datasync: bool, reply: ReplyEmpty) {
        // Get file handle
        let (file_id, db_policy) = match self.file_handles.read().get(&fh) {
            Some(handle) => (handle.file_id, handle.db_policy.clone()),
            None => {
                reply.error(libc::EBADF);
                return;
            }
        };

        // For database files, ensure strong durability guarantees
        if db_policy.write_mode() == crate::database::DatabaseWriteMode::WriteThrough
            || db_policy.write_mode() == crate::database::DatabaseWriteMode::DirectIO
        {
            // Force immediate sync of all pending operations
            let (tx, rx) = oneshot::channel();
            if self
                .operation_tx
                .send(FuseOperation::FlushFile { file_id, reply: tx })
                .is_err()
            {
                reply.error(libc::EIO);
                return;
            }
            let _ = rx.blocking_recv();
        }

        // Force metadata update
        if !datasync {
            let (tx, rx) = oneshot::channel();
            if self
                .operation_tx
                .send(FuseOperation::GetMetadata { file_id, reply: tx })
                .is_err()
            {
                reply.error(libc::EIO);
                return;
            }

            if let Ok(Ok(Some(metadata))) = rx.blocking_recv() {
                let (tx, rx) = oneshot::channel();
                if self
                    .operation_tx
                    .send(FuseOperation::UpdateMetadata {
                        file_id,
                        metadata,
                        reply: tx,
                    })
                    .is_err()
                {
                    reply.error(libc::EIO);
                    return;
                }
                let _ = rx.blocking_recv();
            }
        }

        reply.ok();
    }

    fn opendir(&mut self, _req: &Request, _ino: u64, flags: i32, reply: ReplyOpen) {
        // For directories, we just need to return a file handle
        // No special handling needed for now
        let fh = self.next_file_handle();
        reply.opened(fh, flags as u32);
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        info!("FUSE readdir: ino={ino}, offset={offset}");

        // Get directory ID
        let dir_id = match self.inode_map.read().get_file_id(ino) {
            Some(id) => DirectoryId(id),
            None => {
                error!("Directory not found for inode {ino}");
                reply.error(libc::ENOENT);
                return;
            }
        };

        info!("Sending ListDirectory operation for {dir_id:?}");
        // Send list directory request
        let (tx, rx) = oneshot::channel();
        if self
            .operation_tx
            .send(FuseOperation::ListDirectory { dir_id, reply: tx })
            .is_err()
        {
            error!("Failed to send ListDirectory operation");
            reply.error(libc::EIO);
            return;
        }

        info!("Waiting for ListDirectory response");
        // Wait for response
        let entries = match rx.blocking_recv() {
            Ok(Ok(entries)) => {
                info!("Got {} directory entries", entries.len());
                entries
            }
            Err(e) => {
                error!("Failed to receive directory entries: {e:?}");
                reply.error(libc::EIO);
                return;
            }
            Ok(Err(e)) => {
                error!("Directory listing failed: {e:?}");
                reply.error(libc::EIO);
                return;
            }
        };

        // Add . and .. entries
        let mut all_entries = vec![
            (ino, FuseFileType::Directory, ".".to_string()),
            (ino, FuseFileType::Directory, "..".to_string()),
        ];

        // Add regular entries
        for entry in entries {
            let inode = self.inode_map.write().get_or_create_inode(entry.file_id);
            let file_type = Self::to_fuse_file_type(entry.file_type);
            if let Ok(name) = std::str::from_utf8(&entry.name) {
                all_entries.push((inode, file_type, name.to_string()));
            }
        }

        info!("Sending {} total entries for readdir", all_entries.len());

        // Send entries starting from offset
        for (i, (ino, file_type, name)) in all_entries.iter().enumerate().skip(offset as usize) {
            if reply.add(*ino, (i + 1) as i64, *file_type, name) {
                break;
            }
        }
        info!("READDIR completed successfully");
        reply.ok();
    }

    fn releasedir(&mut self, _req: &Request, _ino: u64, _fh: u64, _flags: i32, reply: ReplyEmpty) {
        // Nothing special needed for directory release
        reply.ok();
    }

    fn create(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        info!("FUSE create: parent={parent}, name={name:?}, mode={mode:o}, flags={flags:x}");
        let name_bytes = name.as_bytes().to_vec();

        // Get parent directory ID
        let parent_id = match self.inode_map.read().get_file_id(parent) {
            Some(id) => DirectoryId(id),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Create new file
        let file_id = FileId::new();
        let metadata = FileMetadata {
            file_id,
            parent_id: Some(parent_id),
            encrypted_name: name_bytes.clone(),
            size: 0,
            blocks: Vec::new(),
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            accessed_at: SystemTime::now(),
            permissions: mode,
            file_type: VsockFileType::Regular,
            nlink: 1,
            uid: req.uid(),
            gid: req.gid(),
        };

        // Send create file request
        let (tx, rx) = oneshot::channel();
        info!("Sending CreateFile operation to async handler");
        if self
            .operation_tx
            .send(FuseOperation::CreateFile {
                parent_id,
                name: name_bytes.clone(),
                metadata: metadata.clone(),
                reply: tx,
            })
            .is_err()
        {
            reply.error(libc::EIO);
            return;
        }

        // Wait for response
        match rx.blocking_recv() {
            Ok(Ok(_)) => {
                // Get inode for new file
                let inode = self.inode_map.write().get_or_create_inode(file_id);

                // Determine database cache policy
                let file_path =
                    std::path::PathBuf::from(std::str::from_utf8(&name_bytes).unwrap_or(""));
                let db_policy = DatabaseCachePolicy::for_file(&file_path, &self.db_detector);
                let direct_io = self.config.filesystem.database_direct_io
                    && db_policy.write_mode() == crate::database::DatabaseWriteMode::DirectIO;

                // Create file handle
                let handle_id = self.next_file_handle();
                let handle = FileHandle {
                    file_id,
                    _flags: flags,
                    _position: 0,
                    _write_buffer: Vec::new(),
                    write_mode: true,
                    db_policy,
                    _direct_io: direct_io,
                };

                self.file_handles.write().insert(handle_id, handle);

                let attr = self.metadata_to_attr(&metadata, inode);
                let ttl = self.config.cache.attr_timeout;
                reply.created(&ttl, &attr, 0, handle_id, 0);
            }
            Ok(Err(_)) | Err(_) => reply.error(libc::EIO),
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_bytes = name.as_bytes().to_vec();

        // Get parent directory ID
        let parent_id = match self.inode_map.read().get_file_id(parent) {
            Some(id) => DirectoryId(id),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Delete file
        let (tx, rx) = oneshot::channel();
        if self
            .operation_tx
            .send(FuseOperation::DeleteFile {
                parent_id,
                name: name_bytes,
                reply: tx,
            })
            .is_err()
        {
            reply.error(libc::EIO);
            return;
        }

        match rx.blocking_recv() {
            Ok(Ok(_)) => reply.ok(),
            _ => reply.error(libc::EIO),
        }
    }

    fn mkdir(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name_bytes = name.as_bytes().to_vec();

        // Get parent directory ID
        let parent_id = match self.inode_map.read().get_file_id(parent) {
            Some(id) => DirectoryId(id),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Create new directory
        let dir_id = FileId::new();
        let metadata = FileMetadata {
            file_id: dir_id,
            parent_id: Some(parent_id),
            encrypted_name: name_bytes.clone(),
            size: 4096,
            blocks: Vec::new(),
            created_at: SystemTime::now(),
            modified_at: SystemTime::now(),
            accessed_at: SystemTime::now(),
            permissions: mode,
            file_type: VsockFileType::Directory,
            nlink: 2,
            uid: req.uid(),
            gid: req.gid(),
        };

        // Create directory
        let (tx, rx) = oneshot::channel();
        if self
            .operation_tx
            .send(FuseOperation::CreateDirectory {
                parent_id,
                name: name_bytes,
                metadata: metadata.clone(),
                reply: tx,
            })
            .is_err()
        {
            reply.error(libc::EIO);
            return;
        }

        match rx.blocking_recv() {
            Ok(Ok(_)) => {
                let inode = self.inode_map.write().get_or_create_inode(dir_id);
                let attr = self.metadata_to_attr(&metadata, inode);
                let ttl = self.config.cache.attr_timeout;
                reply.entry(&ttl, &attr, 0);
            }
            _ => reply.error(libc::EIO),
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_bytes = name.as_bytes().to_vec();

        // Get parent directory ID
        let parent_id = match self.inode_map.read().get_file_id(parent) {
            Some(id) => DirectoryId(id),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Delete directory
        let (tx, rx) = oneshot::channel();
        if self
            .operation_tx
            .send(FuseOperation::DeleteDirectory {
                parent_id,
                name: name_bytes,
                reply: tx,
            })
            .is_err()
        {
            reply.error(libc::EIO);
            return;
        }

        match rx.blocking_recv() {
            Ok(Ok(_)) => reply.ok(),
            _ => reply.error(libc::EIO),
        }
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let old_name = name.as_bytes().to_vec();
        let new_name = newname.as_bytes().to_vec();

        // Get parent directory IDs
        let old_parent = match self.inode_map.read().get_file_id(parent) {
            Some(id) => DirectoryId(id),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let new_parent = match self.inode_map.read().get_file_id(newparent) {
            Some(id) => DirectoryId(id),
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Perform rename
        let (tx, rx) = oneshot::channel();
        if self
            .operation_tx
            .send(FuseOperation::Rename {
                old_parent,
                old_name,
                new_parent,
                new_name,
                reply: tx,
            })
            .is_err()
        {
            reply.error(libc::EIO);
            return;
        }

        match rx.blocking_recv() {
            Ok(Ok(_)) => reply.ok(),
            _ => reply.error(libc::EIO),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inode_map() {
        let mut map = InodeMap::new();

        // Root should be mapped to inode 1
        let root_id = FileId::from_bytes([0; 32]);
        assert_eq!(map.get_or_create_inode(root_id), 1);
        assert_eq!(map.get_file_id(1), Some(root_id));

        // New files should get incrementing inodes
        let file1 = FileId::new();
        let file2 = FileId::new();
        assert_eq!(map.get_or_create_inode(file1), 2);
        assert_eq!(map.get_or_create_inode(file2), 3);

        // Existing files should return same inode
        assert_eq!(map.get_or_create_inode(file1), 2);
    }
}
