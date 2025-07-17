//! Encryption performance benchmarks
//!
//! Measures the performance of encryption operations in isolation.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use proven_vsock_fuse::{
    FileId,
    encryption::{EncryptionLayer, MasterKey},
};

fn bench_key_derivation(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_derivation");

    let master_key = MasterKey::generate();
    let crypto = EncryptionLayer::new(master_key.clone(), 4096).unwrap();

    // Benchmark uncached key derivation (first access)
    group.bench_function("derive_new_file_key", |b| {
        b.iter(|| {
            let file_id = FileId::new();
            black_box(crypto.derive_file_key(&file_id).unwrap());
        });
    });

    // Benchmark cached key derivation (subsequent accesses)
    group.bench_function("derive_cached_file_key", |b| {
        let file_id = FileId::new();
        // Prime the cache
        crypto.derive_file_key(&file_id).unwrap();

        b.iter(|| {
            black_box(crypto.derive_file_key(&file_id).unwrap());
        });
    });

    group.finish();
}

fn bench_block_encryption(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_encryption");

    let master_key = MasterKey::generate();
    let block_sizes = vec![
        (1024, "1KB"),
        (4096, "4KB"),
        (16384, "16KB"),
        (65536, "64KB"),
    ];

    for (block_size, name) in block_sizes {
        group.throughput(Throughput::Bytes(block_size as u64));

        group.bench_with_input(
            BenchmarkId::new("encrypt", name),
            &block_size,
            |b, &block_size| {
                let crypto = EncryptionLayer::new(master_key.clone(), block_size).unwrap();
                let file_id = FileId::new();
                let data = vec![0x42u8; block_size];
                // Prime the key cache
                crypto.derive_file_key(&file_id).unwrap();

                b.iter(|| {
                    black_box(crypto.encrypt_block(&file_id, 0, &data).unwrap());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("decrypt", name),
            &block_size,
            |b, &block_size| {
                let crypto = EncryptionLayer::new(master_key.clone(), block_size).unwrap();
                let file_id = FileId::new();
                let data = vec![0x42u8; block_size];
                // Prime the key cache
                crypto.derive_file_key(&file_id).unwrap();
                let encrypted = crypto.encrypt_block(&file_id, 0, &data).unwrap();

                b.iter(|| {
                    black_box(crypto.decrypt_block(&file_id, 0, &encrypted).unwrap());
                });
            },
        );
    }

    group.finish();
}

fn bench_parallel_encryption(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel_encryption");

    let master_key = MasterKey::generate();
    let block_size = 4096;
    let crypto = EncryptionLayer::new(master_key, block_size).unwrap();

    // Benchmark encrypting multiple blocks as would happen in streaming
    let block_counts = vec![1, 16, 64, 256];

    for count in block_counts {
        let total_size = block_size * count;
        group.throughput(Throughput::Bytes(total_size as u64));

        group.bench_with_input(
            BenchmarkId::new("sequential_blocks", count),
            &count,
            |b, &count| {
                let file_id = FileId::new();
                let block_data = vec![0x42u8; block_size];
                // Prime the key cache
                crypto.derive_file_key(&file_id).unwrap();

                b.iter(|| {
                    for i in 0..count {
                        black_box(
                            crypto
                                .encrypt_block(&file_id, i as u64, &block_data)
                                .unwrap(),
                        );
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_filename_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("filename_operations");

    let master_key = MasterKey::generate();
    let crypto = EncryptionLayer::new(master_key, 4096).unwrap();
    let dir_id = FileId::new();

    let filenames = vec![
        ("short.txt", "short"),
        ("medium_length_filename.txt", "medium"),
        (
            "very_long_filename_with_many_characters_that_might_be_typical_in_real_usage.txt",
            "long",
        ),
    ];

    for (filename, label) in filenames {
        group.bench_with_input(
            BenchmarkId::new("encrypt_filename", label),
            &filename,
            |b, filename| {
                // Prime the directory key cache
                crypto.derive_file_key(&dir_id).unwrap();

                b.iter(|| {
                    black_box(crypto.encrypt_filename(&dir_id, filename).unwrap());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("hash_filename", label),
            &filename,
            |b, filename| {
                // Prime the directory key cache
                crypto.derive_file_key(&dir_id).unwrap();

                b.iter(|| {
                    black_box(crypto.hash_filename(&dir_id, filename));
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_key_derivation,
    bench_block_encryption,
    bench_parallel_encryption,
    bench_filename_operations
);
criterion_main!(benches);
