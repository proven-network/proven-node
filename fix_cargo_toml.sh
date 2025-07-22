#!/bin/bash

# Fix malformed Cargo.toml files where proven-logger-macros line is concatenated

find crates -name "Cargo.toml" -type f | while read -r file; do
    # Fix the concatenated lines by adding newline
    sed -i '' 's/proven-logger-macros\.workspace = true\([a-zA-Z]\)/proven-logger-macros.workspace = true\
\1/g' "$file"
done

echo "Fixed Cargo.toml files"