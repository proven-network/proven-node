# VSOCK-FUSE Benchmarks

This directory contains performance benchmarks for the VSOCK-FUSE filesystem.

## Running Benchmarks

To run all benchmarks:

```bash
./bench.sh
```

To run specific benchmarks:

```bash
# Encryption performance only
cargo bench --bench encryption

# Throughput performance only
cargo bench --bench throughput

# Run with specific filter
cargo bench --bench throughput write_throughput
```

## Benchmark Categories

### Encryption Benchmarks (`encryption.rs`)

Measures the performance of cryptographic operations:

- **Key Derivation**: Tests both cached and uncached file key derivation
- **Block Encryption/Decryption**: Measures throughput for different block sizes (1KB to 64KB)
- **Parallel Encryption**: Tests encrypting multiple blocks sequentially (simulates streaming)
- **Filename Operations**: Benchmarks filename encryption and hashing

### Throughput Benchmarks (`throughput.rs`)

Measures end-to-end filesystem performance:

- **Write Throughput**: Sequential writes of various sizes (4KB to 10MB)
- **Read Throughput**: Sequential reads of various sizes
- **Random I/O**: 4KB random reads/writes on a 10MB file
- **Encryption Overhead**: Compares full write path vs encryption-only

## Performance Metrics

The benchmarks measure:

- **Throughput**: MB/s for data operations
- **Latency**: Time per operation (ns/iter)
- **Scalability**: Performance with different data sizes

## Interpreting Results

- **Key Derivation**: Should show significant speedup for cached vs uncached
- **Block Operations**: Should achieve >1GB/s for encryption/decryption
- **Write Operations**: Performance depends on block count due to streaming optimizations
- **Random I/O**: Typically slower than sequential due to cache misses

## Hardware Considerations

Performance varies based on:

- CPU speed and AES-NI support
- Memory bandwidth
- Storage backend (in-memory for benchmarks)
- Number of CPU cores (for parallel operations)
