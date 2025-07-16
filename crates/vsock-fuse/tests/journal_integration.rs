//! Integration tests for journal and snapshot functionality

use std::sync::Arc;
use tempfile::TempDir;

use proven_vsock_fuse::{
    DirectoryId, FileId, FileMetadata, FileType,
    metadata::{JournalEntry, LocalMetadataStore, MetadataJournal, SnapshotManager},
};

#[test]
fn test_journal_persistence_and_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let journal_path = temp_dir.path();

    // Create initial store and add data
    {
        let store = LocalMetadataStore::with_journal(journal_path).unwrap();

        // Add some files
        for i in 0..5 {
            let file_id = FileId::new();
            let metadata = FileMetadata {
                file_id,
                parent_id: Some(DirectoryId(FileId::from_bytes([0; 32]))),
                encrypted_name: format!("file{i}.txt").into_bytes(),
                size: 100 * (i + 1) as u64,
                blocks: vec![],
                created_at: std::time::SystemTime::now(),
                modified_at: std::time::SystemTime::now(),
                accessed_at: std::time::SystemTime::now(),
                permissions: 0o644,
                file_type: FileType::Regular,
                nlink: 1,
                uid: 1000,
                gid: 1000,
            };
            store.put_file_metadata(metadata);
        }

        // Add directories
        for i in 0..3 {
            let dir_id = DirectoryId(FileId::new());
            store.put_directory(dir_id, vec![]);
            store
                .add_directory_entry(
                    &DirectoryId(FileId::from_bytes([0; 32])),
                    format!("dir{i}").into_bytes(),
                    dir_id.0,
                    FileType::Directory,
                )
                .unwrap();
        }

        // Ensure data is synced
        store.sync_journal().unwrap();
    }

    // Create new store and verify recovery
    {
        let store = LocalMetadataStore::with_journal(journal_path).unwrap();

        // Verify files were recovered
        let stats = store.get_stats();
        // We created 5 files only. Directories are counted separately in total_directories
        assert_eq!(stats.total_files, 5); // Just the 5 regular files we created

        // Verify directory entries
        let root_entries = store
            .get_directory(&DirectoryId(FileId::from_bytes([0; 32])))
            .unwrap();
        assert_eq!(root_entries.len(), 3); // 3 directories we added
    }
}

#[test]
fn test_journal_rotation() {
    let temp_dir = TempDir::new().unwrap();
    let journal_path = temp_dir.path();

    // Create journal with small size to trigger rotation
    let journal = MetadataJournal::new(journal_path, 1024).unwrap(); // 1KB max

    // Write many entries to trigger rotation
    for i in 0..50 {
        let metadata = FileMetadata {
            file_id: FileId::new(),
            parent_id: Some(DirectoryId(FileId::from_bytes([0; 32]))),
            encrypted_name: format!("very_long_filename_to_fill_journal_{i}.txt").into_bytes(),
            size: 1000000,
            blocks: vec![],
            created_at: std::time::SystemTime::now(),
            modified_at: std::time::SystemTime::now(),
            accessed_at: std::time::SystemTime::now(),
            permissions: 0o644,
            file_type: FileType::Regular,
            nlink: 1,
            uid: 1000,
            gid: 1000,
        };

        journal
            .write_entry(JournalEntry::FileMetadata { metadata })
            .unwrap();
    }

    // Check that archive files were created
    let files: Vec<_> = std::fs::read_dir(journal_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name();
            let name_str = name.to_string_lossy();
            name_str.contains("journal")
        })
        .collect();

    assert!(files.len() >= 2, "Expected at least one archive file");
}

#[test]
fn test_snapshot_with_journal_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let journal_path = temp_dir.path();

    // Create store and add initial data
    let store = Arc::new(LocalMetadataStore::with_journal(journal_path).unwrap());

    // Add files that will be in snapshot
    let file_ids: Vec<_> = (0..5)
        .map(|i| {
            let file_id = FileId::new();
            let metadata = FileMetadata {
                file_id,
                parent_id: Some(DirectoryId(FileId::from_bytes([0; 32]))),
                encrypted_name: format!("snapshot_file{i}.txt").into_bytes(),
                size: 100 * i as u64,
                blocks: vec![],
                created_at: std::time::SystemTime::now(),
                modified_at: std::time::SystemTime::now(),
                accessed_at: std::time::SystemTime::now(),
                permissions: 0o644,
                file_type: FileType::Regular,
                nlink: 1,
                uid: 1000,
                gid: 1000,
            };
            store.put_file_metadata(metadata);
            file_id
        })
        .collect();

    // Sync journal before snapshot to ensure all entries are written
    store.sync_journal().unwrap();

    // Create snapshot with current journal sequence
    let current_seq = store.current_journal_sequence().unwrap_or(0);
    // current_seq is the NEXT sequence to be used, so snapshot should be at current_seq - 1
    let snapshot_seq = if current_seq > 0 { current_seq - 1 } else { 0 };
    let snapshot_manager = SnapshotManager::new(journal_path, store.clone());
    let snapshot_path = snapshot_manager.create_snapshot(snapshot_seq).unwrap();

    // Add more files after snapshot
    let post_snapshot_ids: Vec<_> = (0..3)
        .map(|i| {
            let file_id = FileId::new();
            let metadata = FileMetadata {
                file_id,
                parent_id: Some(DirectoryId(FileId::from_bytes([0; 32]))),
                encrypted_name: format!("post_snapshot_file{i}.txt").into_bytes(),
                size: 200 * (i + 1) as u64, // Changed to avoid size 0
                blocks: vec![],
                created_at: std::time::SystemTime::now(),
                modified_at: std::time::SystemTime::now(),
                accessed_at: std::time::SystemTime::now(),
                permissions: 0o644,
                file_type: FileType::Regular,
                nlink: 1,
                uid: 1000,
                gid: 1000,
            };
            store.put_file_metadata(metadata);
            file_id
        })
        .collect();

    // Delete one of the original files
    store.delete_file_metadata(&file_ids[0]);

    // Sync journal
    store.sync_journal().unwrap();

    // Drop the store
    drop(store);

    // Create new store - should recover from snapshot + journal
    let recovered_store = LocalMetadataStore::with_journal(journal_path).unwrap();

    // Verify state
    // - Original file 0 should not exist (it was deleted)
    assert!(recovered_store.get_file_metadata(&file_ids[0]).is_none());

    // - Other original files should exist
    for (i, file_id) in file_ids.iter().skip(1).enumerate() {
        let metadata = recovered_store.get_file_metadata(file_id).unwrap();
        assert_eq!(metadata.size, 100 * (i + 1) as u64);
    }

    // - Post-snapshot files should exist
    for (i, file_id) in post_snapshot_ids.iter().enumerate() {
        let metadata = recovered_store.get_file_metadata(file_id).unwrap();
        assert_eq!(metadata.size, 200 * (i + 1) as u64);
    }

    // Verify snapshot file still exists
    assert!(snapshot_path.exists());
}

#[test]
fn test_concurrent_journal_writes() {
    use std::sync::Arc;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let journal_path = temp_dir.path();
    let store = Arc::new(LocalMetadataStore::with_journal(journal_path).unwrap());

    // Spawn multiple threads writing to the journal
    let threads: Vec<_> = (0..10)
        .map(|thread_id| {
            let store = store.clone();
            thread::spawn(move || {
                for i in 0..10 {
                    let file_id = FileId::new();
                    let metadata = FileMetadata {
                        file_id,
                        parent_id: Some(DirectoryId(FileId::from_bytes([0; 32]))),
                        encrypted_name: format!("thread{thread_id}_file{i}.txt").into_bytes(),
                        size: (thread_id * 100 + i) as u64,
                        blocks: vec![],
                        created_at: std::time::SystemTime::now(),
                        modified_at: std::time::SystemTime::now(),
                        accessed_at: std::time::SystemTime::now(),
                        permissions: 0o644,
                        file_type: FileType::Regular,
                        nlink: 1,
                        uid: 1000,
                        gid: 1000,
                    };
                    store.put_file_metadata(metadata);
                }
            })
        })
        .collect();

    // Wait for all threads
    for t in threads {
        t.join().unwrap();
    }

    // Sync and verify
    store.sync_journal().unwrap();
    let stats = store.get_stats();
    assert_eq!(stats.total_files, 100); // 100 files (root is not counted in total_files)
}

#[test]
fn test_journal_compaction() {
    let temp_dir = TempDir::new().unwrap();
    let journal_path = temp_dir.path();
    let store = LocalMetadataStore::with_journal(journal_path).unwrap();

    // Create and update the same file multiple times
    let file_id = FileId::new();
    for i in 0..10 {
        let metadata = FileMetadata {
            file_id,
            parent_id: Some(DirectoryId(FileId::from_bytes([0; 32]))),
            encrypted_name: b"compaction_test.txt".to_vec(),
            size: 100 * (i + 1),
            blocks: vec![],
            created_at: std::time::SystemTime::now(),
            modified_at: std::time::SystemTime::now(),
            accessed_at: std::time::SystemTime::now(),
            permissions: 0o644,
            file_type: FileType::Regular,
            nlink: 1,
            uid: 1000,
            gid: 1000,
        };
        store.put_file_metadata(metadata);
    }

    // Also add a directory entry to make the journal larger
    store
        .add_directory_entry(
            &DirectoryId(FileId::from_bytes([0; 32])),
            b"compaction_test.txt".to_vec(),
            file_id,
            FileType::Regular,
        )
        .unwrap();

    // Sync journal before checking size
    store.sync_journal().unwrap();

    // Get journal file size before compaction
    let journal_file = journal_path.join("metadata.journal");
    let size_before = std::fs::metadata(&journal_file)
        .expect("Journal file should exist")
        .len();

    // Compact journal
    store.compact_journal(0).unwrap();

    // Get size after compaction
    let size_after = std::fs::metadata(&journal_file)
        .expect("Journal file should exist after compaction")
        .len();

    // Journal should be smaller after compaction (10 entries -> 1 entry)
    assert!(
        size_after < size_before,
        "Journal should be smaller after compaction: before={size_before}, after={size_after}"
    );

    // Verify data is still correct
    let metadata = store.get_file_metadata(&file_id).unwrap();
    assert_eq!(metadata.size, 1000); // Last update was size 1000
}
