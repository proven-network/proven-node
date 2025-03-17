#!/bin/bash
set -e

# Use the shared topology file
TOPOLOGY_FILE="/etc/proven/topology.json"

echo "Starting node with ID: $NODE_ID, port: $PORT"

# Create directories if they don't exist
mkdir -p /etc/proven /var/lib/proven /tmp/proven

# Display which private key is being used
echo "Using private key: ${PRIVATE_KEY}..."

# Ensure the hosts file has the right entries if needed
if [ -f "/tmp/proven/hosts" ]; then
  cat /tmp/proven/hosts > /etc/hosts
fi

# Run the proven-local binary
echo "Starting proven-local with topology file: $TOPOLOGY_FILE and private key via environment variable on port $PORT"
exec proven-local --topology-file $TOPOLOGY_FILE
