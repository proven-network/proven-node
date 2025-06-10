#!/bin/bash
# Script to run the Proven Node cluster

set -e

# Default log level
RUST_LOG="info"

# Parse named arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --log)
      RUST_LOG="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

echo "============================"
echo "Starting Proven Node Cluster"
echo "Log level: ${RUST_LOG}"
echo "============================"


# Set up UPnP port forwarding
./upnp-setup.sh

# export variables for docker-compose
export COMPOSE_DOCKER_CLI_BUILD=1
export DOCKER_BUILDKIT=1
export RUST_LOG

# Build and run the Docker containers
cd integration-test

echo "Building containers..."
docker-compose build

echo "Starting node cluster"
# Run only the node containers, not the test-client
docker-compose up auth-gateway bulbasaur charmander squirtle

# This line will only be reached when the user stops the cluster with Ctrl+C
echo "Cluster stopped. Cleaning up..."

# Clean up UPnP port forwarding
./../upnp-teardown.sh

echo "Shutdown complete" 
