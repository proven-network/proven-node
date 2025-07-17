#!/bin/bash
# Run vsock-fuse benchmarks

echo "Running VSOCK-FUSE Benchmarks..."
echo "================================"

# Run encryption benchmarks
echo -e "\n🔐 Encryption Performance"
echo "-------------------------"
cargo bench --bench encryption -- --noplot

# Run throughput benchmarks
echo -e "\n📊 Throughput Performance"
echo "-------------------------"
cargo bench --bench throughput -- --noplot

echo -e "\nBenchmarks complete!"
echo "Results saved in target/criterion/"
