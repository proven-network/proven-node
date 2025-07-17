//! Throughput benchmarks for VSOCK-FUSE
//!
//! Measures read and write performance for various file sizes and patterns.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tempfile::TempDir;
use tokio::runtime::Runtime;

use proven_vsock_fuse::{
    DirectoryId, FileId, FileMetadata, FileType,
    encryption::{EncryptionLayer, MasterKey},
    fuse_async::{FuseAsyncHandler, FuseOperation},
    metadata::LocalEncryptedMetadataStore,
};

use proven_vsock_fuse::storage::mock::MockBlobStorage;

struct BenchmarkSetup {
    tx: tokio::sync::mpsc::UnboundedSender<FuseOperation>,
    runtime: Runtime,
    _temp_dir: TempDir,
}

impl BenchmarkSetup {
    fn new() -> Self {
        let runtime = Runtime::new().unwrap();
        let temp_dir = TempDir::new().unwrap();
        let master_key = MasterKey::generate();
        let crypto = Arc::new(EncryptionLayer::new(master_key, 4096).unwrap());
        let storage = Arc::new(MockBlobStorage::new());

        let metadata_store = Arc::new(
            LocalEncryptedMetadataStore::with_journal(crypto.clone(), None, temp_dir.path())
                .unwrap(),
        );

        let handler = Arc::new(FuseAsyncHandler::new(metadata_store, storage, crypto, 4096));

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<FuseOperation>();

        // Spawn handler task
        let handler_clone = handler.clone();
        runtime.spawn(async move {
            handler_clone.run(rx).await;
        });

        Self {
            tx,
            runtime,
            _temp_dir: temp_dir,
        }
    }

    fn create_file(&self, name: &str) -> FileId {
        let file_id = FileId::new();
        let parent_id = DirectoryId(FileId::from_bytes([0; 32]));

        let metadata = FileMetadata {
            file_id,
            parent_id: Some(parent_id),
            encrypted_name: name.as_bytes().to_vec(),
            size: 0,
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

        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(FuseOperation::CreateFile {
                parent_id,
                name: name.as_bytes().to_vec(),
                metadata,
                reply: reply_tx,
            })
            .unwrap();

        self.runtime.block_on(reply_rx).unwrap().unwrap();
        file_id
    }

    fn write_data(&self, file_id: FileId, offset: u64, data: Vec<u8>) -> usize {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(FuseOperation::WriteFile {
                file_id,
                offset,
                data,
                reply: reply_tx,
            })
            .unwrap();

        self.runtime.block_on(reply_rx).unwrap().unwrap()
    }

    fn read_data(&self, file_id: FileId, offset: u64, size: u32) -> Vec<u8> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(FuseOperation::ReadFile {
                file_id,
                offset,
                size,
                reply: reply_tx,
            })
            .unwrap();

        self.runtime.block_on(reply_rx).unwrap().unwrap()
    }
}

fn bench_write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_throughput");

    // Test different sizes: 4KB, 64KB, 1MB, 10MB
    let sizes = vec![
        (4 * 1024, "4KB"),
        (64 * 1024, "64KB"),
        (1024 * 1024, "1MB"),
        (10 * 1024 * 1024, "10MB"),
    ];

    for (size, name) in sizes {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("sequential", name), &size, |b, &size| {
            let setup = BenchmarkSetup::new();
            let data = vec![0x42u8; size];

            b.iter(|| {
                let file_id = setup.create_file(&format!("bench_write_{size}.dat"));
                black_box(setup.write_data(file_id, 0, data.clone()));
            });
        });
    }

    group.finish();
}

fn bench_read_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_throughput");

    // Test different sizes
    let sizes = vec![
        (4 * 1024, "4KB"),
        (64 * 1024, "64KB"),
        (1024 * 1024, "1MB"),
        (10 * 1024 * 1024, "10MB"),
    ];

    for (size, name) in sizes {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("sequential", name), &size, |b, &size| {
            let setup = BenchmarkSetup::new();

            // Pre-write the file
            let file_id = setup.create_file(&format!("bench_read_{size}.dat"));
            let data = vec![0x42u8; size];
            setup.write_data(file_id, 0, data);

            b.iter(|| {
                black_box(setup.read_data(file_id, 0, size as u32));
            });
        });
    }

    group.finish();
}

fn bench_random_io(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_io");

    // Test 4KB random reads/writes on a 10MB file
    let file_size = 10 * 1024 * 1024;
    let block_size = 4096;
    let num_blocks = file_size / block_size;

    group.throughput(Throughput::Bytes(block_size as u64));

    group.bench_function("random_read_4kb", |b| {
        let setup = BenchmarkSetup::new();

        // Pre-write a large file
        let file_id = setup.create_file("random_io.dat");
        let data = vec![0x42u8; file_size];
        setup.write_data(file_id, 0, data);

        // Use a simple LCG for deterministic "random" offsets
        let mut rng_state = 12345u64;

        b.iter(|| {
            // Simple LCG: next = (a * current + c) % m
            rng_state =
                (rng_state.wrapping_mul(1664525).wrapping_add(1013904223)) % num_blocks as u64;
            let offset = rng_state * block_size as u64;
            black_box(setup.read_data(file_id, offset, block_size as u32));
        });
    });

    group.bench_function("random_write_4kb", |b| {
        let setup = BenchmarkSetup::new();
        let file_id = setup.create_file("random_write.dat");
        let data = vec![0x42u8; block_size];

        // Pre-allocate the file
        let initial_data = vec![0u8; file_size];
        setup.write_data(file_id, 0, initial_data);

        let mut rng_state = 12345u64;

        b.iter(|| {
            rng_state =
                (rng_state.wrapping_mul(1664525).wrapping_add(1013904223)) % num_blocks as u64;
            let offset = rng_state * block_size as u64;
            black_box(setup.write_data(file_id, offset, data.clone()));
        });
    });

    group.finish();
}

fn bench_encryption_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("encryption_overhead");

    // Compare encrypted vs raw throughput for different block counts
    let block_size = 4096;
    let block_counts = vec![(1, "1_block"), (16, "16_blocks"), (256, "256_blocks")];

    for (count, name) in block_counts {
        let size = block_size * count;
        group.throughput(Throughput::Bytes(size as u64));

        // Benchmark the full encrypted write path
        group.bench_with_input(
            BenchmarkId::new("encrypted_write", name),
            &size,
            |b, &size| {
                let setup = BenchmarkSetup::new();
                let data = vec![0x42u8; size];

                b.iter(|| {
                    let file_id = setup.create_file(&format!("encrypted_{size}.dat"));
                    black_box(setup.write_data(file_id, 0, data.clone()));
                });
            },
        );

        // Benchmark just the encryption layer
        group.bench_with_input(
            BenchmarkId::new("encryption_only", name),
            &count,
            |b, &count| {
                let master_key = MasterKey::generate();
                let crypto = EncryptionLayer::new(master_key, block_size).unwrap();
                let file_id = FileId::new();
                let data = vec![0x42u8; block_size];

                b.iter(|| {
                    for block_num in 0..count {
                        black_box(
                            crypto
                                .encrypt_block(&file_id, block_num as u64, &data)
                                .unwrap(),
                        );
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_write_throughput,
    bench_read_throughput,
    bench_random_io,
    bench_encryption_overhead
);
criterion_main!(benches);
