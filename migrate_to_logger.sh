#!/bin/bash

# Script to migrate crates from tracing to proven-logger

# List of crates that need migration (excluding logger crates and already migrated ones)
CRATES=(
    "code-package"
    "core"
    "dnscrypt-proxy"
    "enclave"
    "engine"
    "ethereum-lighthouse"
    "ethereum-reth"
    "external-fs"
    "host"
    "http-insecure"
    "http-letsencrypt"
    "http-proxy"
    "identity"
    "isolation"
    "local"
    "local-cli"
    "local-tui"
    "locks-memory"
    "messaging-engine"
    "messaging-memory"
    "network"
    "postgres"
    "radix-aggregator"
    "radix-gateway"
    "radix-node"
    "radix-rola"
    "radix-stream"
    "runtime"
    "sessions"
    "sql-streamed"
    "storage"
    "storage-memory"
    "storage-rocksdb"
    "storage-s3"
    "store-engine"
    "topology-governance"
    "transport-memory"
    "transport-tcp"
    "transport-ws"
    "verification"
    "vsock-cac"
    "vsock-fuse"
    "vsock-proxy"
    "vsock-rpc"
)

echo "Starting migration of ${#CRATES[@]} crates from tracing to proven-logger..."

for crate in "${CRATES[@]}"; do
    echo ""
    echo "=== Processing $crate ==="
    
    CARGO_TOML="crates/$crate/Cargo.toml"
    
    if [ ! -f "$CARGO_TOML" ]; then
        echo "WARNING: $CARGO_TOML not found, skipping..."
        continue
    fi
    
    # Replace tracing dependency with proven-logger
    sed -i '' 's/tracing\.workspace = true/proven-logger.workspace = true/g' "$CARGO_TOML"
    
    # Add proven-logger-macros to dev-dependencies if there are tests
    if grep -q '\[dev-dependencies\]' "$CARGO_TOML"; then
        # Check if it's not already added
        if ! grep -q 'proven-logger-macros' "$CARGO_TOML"; then
            # Add after [dev-dependencies] line
            sed -i '' '/\[dev-dependencies\]/a\
proven-logger-macros.workspace = true' "$CARGO_TOML"
        fi
    fi
    
    # Remove tracing-subscriber from dev-dependencies
    sed -i '' '/tracing-subscriber\.workspace = true/d' "$CARGO_TOML"
    
    # Process all Rust source files
    find "crates/$crate/src" -name "*.rs" -type f | while read -r file; do
        # Replace use statements
        sed -i '' 's/use tracing::{debug, error, info, warn}/use proven_logger::{debug, error, info, warn}/g' "$file"
        sed -i '' 's/use tracing::{debug, error, info}/use proven_logger::{debug, error, info}/g' "$file"
        sed -i '' 's/use tracing::{debug, info}/use proven_logger::{debug, info}/g' "$file"
        sed -i '' 's/use tracing::{error, info}/use proven_logger::{error, info}/g' "$file"
        sed -i '' 's/use tracing::{warn}/use proven_logger::{warn}/g' "$file"
        sed -i '' 's/use tracing::{info}/use proven_logger::{info}/g' "$file"
        sed -i '' 's/use tracing::{debug}/use proven_logger::{debug}/g' "$file"
        sed -i '' 's/use tracing::{error}/use proven_logger::{error}/g' "$file"
        sed -i '' 's/use tracing::{trace}/use proven_logger::{trace}/g' "$file"
        
        # Replace standalone tracing:: calls
        sed -i '' 's/tracing::debug!/debug!/g' "$file"
        sed -i '' 's/tracing::error!/error!/g' "$file"
        sed -i '' 's/tracing::info!/info!/g' "$file"
        sed -i '' 's/tracing::warn!/warn!/g' "$file"
        sed -i '' 's/tracing::trace!/trace!/g' "$file"
        
        # Handle target: syntax by converting to prefix
        sed -i '' 's/debug!(target: "\([^"]*\)", /debug!("\1: /g' "$file"
        sed -i '' 's/error!(target: "\([^"]*\)", /error!("\1: /g' "$file"
        sed -i '' 's/info!(target: "\([^"]*\)", /info!("\1: /g' "$file"
        sed -i '' 's/warn!(target: "\([^"]*\)", /warn!("\1: /g' "$file"
        sed -i '' 's/trace!(target: "\([^"]*\)", /trace!("\1: /g' "$file"
    done
    
    # Process test files for traced_test macro
    find "crates/$crate" -name "*.rs" -type f | while read -r file; do
        # Replace traced_test with logged_test
        sed -i '' 's/#\[traced_test\]/#\[logged_test\]/g' "$file"
        sed -i '' 's/use tracing_test::traced_test/use proven_logger_macros::logged_test/g' "$file"
        
        # Replace tokio traced tests
        sed -i '' 's/#\[traced_test(tokio::test)\]/#\[logged_tokio_test\]/g' "$file"
    done
    
    echo "Processed $crate"
done

echo ""
echo "Migration complete! Now checking each crate..."

# Check each crate compiles
FAILED_CRATES=()
for crate in "${CRATES[@]}"; do
    echo -n "Checking $crate... "
    if cd "crates/$crate" && cargo check --quiet 2>/dev/null; then
        echo "✓"
    else
        echo "✗"
        FAILED_CRATES+=("$crate")
    fi
    cd - > /dev/null
done

if [ ${#FAILED_CRATES[@]} -gt 0 ]; then
    echo ""
    echo "The following crates failed to compile:"
    for crate in "${FAILED_CRATES[@]}"; do
        echo "  - $crate"
    done
else
    echo ""
    echo "All crates compiled successfully!"
fi